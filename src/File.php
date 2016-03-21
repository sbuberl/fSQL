<?php

namespace FSQL;

/* A reentrant read write lock for a file */
class File
{
    protected $handle;
    private $filePath;

    public function __construct($filePath)
    {
        $this->filePath = $filePath;
        $this->handle = null;
    }

    public function __destruct()
    {
        // should be unlocked before reaches here, but just in case,
        // release all locks and close file
        if (isset($this->handle)) {
            $this->close();
        }
    }

    public function getHandle()
    {
        return $this->handle;
    }

    public function getPath()
    {
        return $this->filePath;
    }

    public function exists()
    {
        return file_exists($this->filePath);
    }

    public function open($mode)
    {
        $this->handle = fopen($this->filePath, $mode);

        return $this->handle !== false;
    }

    public function lock($mode)
    {
        flock($this->handle, $mode);
    }

    public function close()
    {
        if ($this->handle) {
            flock($this->handle, LOCK_UN);
            fclose($this->handle);
            $this->handle = null;
        }
    }

    public function drop()
    {
        // only allow drops if not close
        if (!$this->handle) {
            unlink($this->filePath);

            return true;
        } else {
            return false;
        }
    }
}
