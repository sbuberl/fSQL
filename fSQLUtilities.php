<?php

/* A reentrant read write lock for a file */
class fSQLFile
{
    var $handle;
    var $filepath;
    var $lock;
    var $rcount = 0;
    var $wcount = 0;

    function fSQLFile($filepath)
    {
        $this->filepath = $filepath;
        $this->handle = null;
        $this->lock = 0;
    }

    function close()
    {
        // should be unlocked before reaches here, but just in case,
        // release all locks and close file
        if(isset($this->handle)) {
            // flock($this->handle, LOCK_UN);
            fclose($this->handle);
        }
        unset($this->filepath, $this->handle, $this->lock, $this->rcount, $this->wcount);
    }

    function exists()
    {
        return file_exists($this->filepath);
    }

    function drop()
    {
        // only allow drops if not locked
        if($this->handle === null)
        {
            unlink($this->filepath);
            $this->close();
            return true;
        }
        else
            return false;
    }

    function getHandle()
    {
        return $this->handle;
    }

    function getPath()
    {
        return $this->filepath;
    }

    function acquireRead()
    {
        if($this->lock !== 0 && $this->handle !== null) {  /* Already have at least a read lock */
            $this->rcount++;
            return true;
        }
        else if($this->lock === 0 && $this->handle === null) /* New lock */
        {
            $this->handle = fopen($this->filepath, 'rb');
            if($this->handle)
            {
                flock($this->handle, LOCK_SH);
                $this->lock = 1;
                $this->rcount = 1;
                return true;
            }
        }

        return false;
    }

    function acquireWrite()
    {
        if($this->lock === 2 && $this->handle !== null)  /* Already have a write lock */
        {
            $this->wcount++;
            return true;
        }
        else if($this->lock === 1 && $this->handle !== null)  /* Upgrade a lock*/
        {
            flock($this->handle, LOCK_EX);
            $this->lock = 2;
            $this->wcount++;
            return true;
        }
        else if($this->lock === 0 && $this->handle === null) /* New lock */
        {
            touch($this->filepath); // make sure it exists
            $this->handle = fopen($this->filepath, 'r+b');
            if($this->handle)
            {
                flock($this->handle, LOCK_EX);
                $this->lock = 2;
                $this->wcount = 1;
                return true;
            }
        }

        return false;
    }

    function releaseRead()
    {
        if($this->lock !== 0 && $this->handle !== null)
        {
            $this->rcount--;

            if($this->lock === 1 && $this->rcount === 0) /* Read lock now empty */
            {
                // no readers or writers left, release lock
                flock($this->handle, LOCK_UN);
                fclose($this->handle);
                $this->handle = null;
                $this->lock = 0;
            }
        }

        return true;
    }

    function releaseWrite()
    {
        if($this->lock !== 0 && $this->handle !== null)
        {
            if($this->lock === 2) /* Write lock */
            {
                $this->wcount--;
                if($this->wcount === 0) // no writers left.
                {
                    if($this->rcount > 0)  // only readers left.  downgrade lock.
                    {
                        flock($this->handle, LOCK_SH);
                        $this->lock = 1;
                    }
                    else // no readers or writers left, release lock
                    {
                        flock($this->handle, LOCK_UN);
                        fclose($this->handle);
                        $this->handle = null;
                        $this->lock = 0;
                    }
                }
            }
        }

        return true;
    }
}

class fSQLMicrotimeLockFile extends fSQLFile
{
    var $loadTime = null;
    var $lastReadStamp = null;

    function accept()
    {
        $this->loadTime = $this->lastReadStamp;
    }

    function close()
    {
        unset($this->loadTime, $this->lastReadStamp);
        parent::close();
    }

    function reset()
    {
        $this->loadTime = null;
        $this->lastReadStamp = null;
        return true;
    }

    function wasModified()
    {
        $this->acquireRead();

        $this->lastReadStamp = fread($this->handle, 20);
        $modified = $this->loadTime === null || $this->loadTime < $this->lastReadStamp;

        $this->releaseRead();

        return $modified;
    }

    function wasNotModified()
    {
        $this->acquireRead();

        $this->lastReadStamp = fread($this->handle, 20);
        $modified = $this->loadTime === null || $this->loadTime >= $this->lastReadStamp;

        $this->releaseRead();

        return $modified;
    }

    function write()
    {
        $this->acquireWrite();

        list($msec, $sec) = explode(' ', microtime());
        $this->loadTime = $sec.$msec;
        ftruncate($this->handle, 0);
        fwrite($this->handle, $this->loadTime);

        $this->releaseWrite();
    }
}

?>
