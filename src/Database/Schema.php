<?php

namespace FSQL\Database;

use FSQL\Utilities;

class Schema
{
    private $name = null;
    private $path = null;
    private $database = null;
    private $loadedTables = array();
    private $sequencesFile;

    public function __construct(Database $database, $name)
    {
        $this->database = $database;
        $this->name = $name;
        $this->path = $name !== 'public' ? $database->path().$name.'/' : $database->path();
        $this->sequencesFile = new SequencesFile($this);
    }

    public function name()
    {
        return $this->name;
    }

    public function fullName()
    {
        return $this->database->name().'.'.$this->name;
    }

    public function path()
    {
        return $this->path;
    }

    public function database()
    {
        return $this->database;
    }

    public function create()
    {
        if ($this->name === 'public') {
            return true;
        }

        $path = Utilities::createDirectory($this->path, 'schema', $this->database->environment());
        if ($path !== false) {
            $this->path = $path;

            return true;
        } else {
            return false;
        }
    }

    public function drop()
    {
        $tables = $this->listTables();

        foreach ($tables as $table) {
            $this->dropTable($table);
        }

        if ($this->sequencesFile->exists()) {
            $this->sequencesFile->drop();
        }

        if ($this->name !== 'public') {
            Utilities::deleteDirectory($this->path);
        }
    }

    public function createTable($table_name, array $columns, $temporary = false)
    {
        if (!$temporary) {
            return CachedTable::create($this, $table_name, $columns);
        } else {
            $table = TempTable::create($this, $table_name, $columns);
            $this->loadedTables[$table_name] = $table;

            return $table;
        }
    }

    public function getRelation($name)
    {
        $table = $this->getTable($name);
        if ($table->exists()) {
            return $table;
        }

        $sequence = $this->getSequence($name);
        if ($sequence !== false) {
            return $sequence;
        }

        return false;
    }

    public function getSequence($name)
    {
        return $this->sequencesFile->getSequence($name);
    }

    public function getTable($table_name)
    {
        if (!isset($this->loadedTables[$table_name])) {
            $table = new CachedTable($this, $table_name);
            if($table->exists())
            {
                $this->loadedTables[$table_name] = $table;
            }
            return $table;
        }

        return $this->loadedTables[$table_name];
    }

    public function getSequences()
    {
        return $this->sequencesFile;
    }

    public function listTables()
    {
        $tables = array();
        if (is_dir($this->path)) {
            $dir = new \DirectoryIterator($this->path);
            foreach ($dir as $file) {
                $fileName = $file->getFilename();
                if (!$file->isDir() && substr($fileName, -12) == '.columns.cgi') {
                    $tables[] = substr($fileName, 0, -12);
                }
            }
        }

        asort($tables);
        return $tables;
    }

    public function renameTable($old_table_name, $new_table_name, Schema $new_schema)
    {
        $oldTable = $this->getTable($old_table_name);
        if ($oldTable->exists()) {
            if (!$oldTable->temporary()) {
                $newTable = $new_schema->createTable($new_table_name, $oldTable->getColumns());
                copy($oldTable->dataFile->getPath(), $newTable->dataFile->getPath());
                copy($oldTable->dataLockFile->getPath(), $newTable->dataLockFile->getPath());
                $this->dropTable($old_table_name);
            } else {
                $new_schema->loadedTables[$new_table_name] = $this->loadedTables[$old_table_name];
                unset($this->loadedTables[$old_table_name]);
            }

            return true;
        } else {
            return false;
        }
    }

    public function dropTable($table_name)
    {
        $table = $this->getTable($table_name);
        if ($table->exists()) {
            $table->drop();
            unset($this->loadedTables[$table_name]);

            return true;
        } else {
            return false;
        }
    }
}
