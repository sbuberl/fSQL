<?php

namespace FSQL\Database;

use FSQL\File;
use FSQL\LockableFile;
use FSQL\MicrotimeLockFile;

class SequencesFile
{
    private $schema;
    private $sequences;
    private $file;
    public $lockFile;

    public function __construct(Schema $schema)
    {
        $this->schema = $schema;
        $path = $schema->path().'sequences';
        $this->sequences = array();
        $this->file = new LockableFile(new File($path.'.cgi'));
        $this->lockFile = new MicrotimeLockFile(new File($path.'.lock.cgi'));
    }

    public function create()
    {
        $this->lockFile->write();
        $this->lockFile->reset();

        $this->file->acquireWrite();
        fwrite($this->file->getHandle(), '');
        $this->file->releaseWrite();

        return true;
    }

    public function schema()
    {
        return $this->schema;
    }

    public function exists()
    {
        return $this->file->exists();
    }

    public function isEmpty()
    {
        return empty($this->sequences);
    }

    public function drop()
    {
        $this->lockFile->drop();
        $this->file->drop();
    }

    public function addSequence($name, $start, $increment, $min, $max, $cycle)
    {
        $this->lockFile->acquireWrite();
        $this->file->acquireWrite();

        $this->reload();

        $sequence = new Sequence($name, $this);
        $sequence->set($start, $start, $increment, $min, $max, $cycle);
        $this->sequences[$name] = $sequence;

        $fileHandle = $this->file->getHandle();
        fseek($fileHandle, 0, SEEK_END);
        fprintf($fileHandle, "%s: %d;%d;%d;%d;%d;%d\r\n", $name, $start,
            $start, $increment, $min, $max, $cycle);

        $this->file->releaseWrite();
        $this->lockFile->releaseWrite();
    }

    public function getSequence($name)
    {
        $sequence = false;
        if ($this->exists()) {
            $this->lockFile->acquireRead();
            $this->reload();

            if (isset($this->sequences[$name])) {
                $sequence = $this->sequences[$name];
            }
            $this->lockFile->releaseRead();
        }

        return $sequence;
    }

    public function dropSequence($name)
    {
        $this->lockFile->acquireWrite();
        $this->reload();
        if (isset($this->sequences[$name])) {
            unset($this->sequences[$name]);
        }

        $this->save();
        $this->lockFile->releaseWrite();
    }

    public function reload()
    {
        $this->lockFile->acquireRead();
        if ($this->lockFile->wasModified()) {
            $this->lockFile->accept();

            $this->file->acquireRead();
            $fileHandle = $this->file->getHandle();
            while (!feof($fileHandle)) {
                fscanf($fileHandle, "%[^:]: %d;%d;%d;%d;%d;%d\r\n", $name, $current,
                    $start, $increment, $min, $max, $cycle);
                if (!isset($this->sequences[$name])) {
                    $this->sequences[$name] = new Sequence($name, $this);
                }
                $this->sequences[$name]->set($current, $start, $increment, $min, $max, $cycle);
            }

            $this->file->releaseRead();
        }
        $this->lockFile->releaseRead();
    }

    public function save()
    {
        $this->lockFile->acquireWrite();
        $this->file->acquireWrite();

        $this->lockFile->write();

        $fileHandle = $this->file->getHandle();
        ftruncate($fileHandle, 0);
        foreach ($this->sequences as $name => $sequence) {
            fprintf($fileHandle, "%s: %d;%d;%d;%d;%d;%d\r\n", $name, $sequence->current,
                $sequence->start, $sequence->increment, $sequence->min, $sequence->max, $sequence->cycle);
        }

        $this->file->releaseWrite();
        $this->lockFile->releaseWrite();
    }
}
