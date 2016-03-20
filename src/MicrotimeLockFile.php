<?php

namespace FSQL;

class MicrotimeLockFile extends File
{
    private $loadTime = null;
    private $lastReadStamp = null;

    public function __destruct()
    {
        parent::__destruct();
    }

    public function accept()
    {
        $this->loadTime = $this->lastReadStamp;
    }

    public function reset()
    {
        $this->loadTime = null;
        $this->lastReadStamp = null;

        return true;
    }

    public function wasModified()
    {
        $this->acquireRead();

        $this->lastReadStamp = fread($this->handle, 20);
        $modified = $this->loadTime === null || $this->loadTime < $this->lastReadStamp;

        $this->releaseRead();

        return $modified;
    }

    public function wasNotModified()
    {
        $this->acquireRead();

        $this->lastReadStamp = fread($this->handle, 20);
        $modified = $this->loadTime === null || $this->loadTime >= $this->lastReadStamp;

        $this->releaseRead();

        return $modified;
    }

    public function write()
    {
        $this->acquireWrite();

        list($msec, $sec) = explode(' ', microtime());
        $this->loadTime = $sec.$msec;
        ftruncate($this->handle, 0);
        fwrite($this->handle, $this->loadTime);

        $this->releaseWrite();
    }
}
