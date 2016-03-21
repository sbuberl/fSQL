<?php

namespace FSQL;

class MicrotimeLockFile extends LockableFile
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

        $this->lastReadStamp = fread($this->getHandle(), 20);
        $modified = $this->loadTime === null || $this->loadTime < $this->lastReadStamp;

        $this->releaseRead();

        return $modified;
    }

    public function wasNotModified()
    {
        $this->acquireRead();

        $this->lastReadStamp = fread($this->getHandle(), 20);
        $modified = $this->loadTime === null || $this->loadTime >= $this->lastReadStamp;

        $this->releaseRead();

        return $modified;
    }

    public function write()
    {
        $this->acquireWrite();

        list($msec, $sec) = explode(' ', microtime());
        $this->loadTime = $sec.$msec;
        $handle = $this->getHandle();
        ftruncate($handle, 0);
        fwrite($handle, $this->loadTime);

        $this->releaseWrite();
    }
}
