<?php

namespace FSQL\Database;

use FSQL\Environment;
use FSQL\Utilities;

class Database
{
    private $name = null;
    private $path = null;
    private $environment = null;
    private $schemas = array();

    public function __construct(Environment $environment, $name, $filePath)
    {
        $this->environment = $environment;
        $this->name = $name;
        $this->path = $filePath;
    }

    public function name()
    {
        return $this->name;
    }

    public function path()
    {
        return $this->path;
    }

    public function environment()
    {
        return $this->environment;
    }

    public function create()
    {
        $path = Utilities::createDirectory($this->path, 'database', $this->environment);
        if ($path !== false) {
            $this->path = $path;

            foreach ($this->listSchemas() as $schemaName) {
                $this->defineSchema($schemaName);
            }

            return true;
        } else {
            return false;
        }
    }

    public function drop()
    {
        foreach ($this->schemas as $schema) {
            $schema->drop();
        }
        $this->schemas = array();
    }

    public function defineSchema($name)
    {
        if (!isset($this->schemas[$name])) {
            $schema = new Schema($this, $name);
            if ($schema->create()) {
                $this->schemas[$name] = $schema;
            } else {
                return false;
            }
        }

        return true;
    }

    public function getSchema($name)
    {
        if (!isset($this->schemas[$name])) {
            if (in_array($name, $this->listSchemas())) {
                $this->schemas[$name] = new Schema($this, $name);
            } else {
                return false;
            }
        }

        return $this->schemas[$name];
    }

    public function listSchemas()
    {
        $schemas = array('public');
        $dir = new \DirectoryIterator($this->path);
        foreach ($dir as $file) {
            if ($file->isDir() && !$file->isDot()) {
                $schemas[] = $file->getFilename();
            }
        }

        return $schemas;
    }

    public function dropSchema($name)
    {
        if (isset($this->schemas[$name])) {
            $this->schemas[$name]->drop();
            unset($this->schemas[$name]);

            return true;
        }

        return false;
    }
}
