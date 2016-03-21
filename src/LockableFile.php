<?php

namespace FSQL;

/* A reentrant read write lock for a file */
class LockableFile
{
    protected $file;
    private $lock;
    private $rcount = 0;
    private $wcount = 0;

    public function __construct(File $file)
    {
        $this->file = $file;
        $this->lock = LOCK_UN;
    }

    public function __destruct()
    {
        // should be unlocked before reaches here, but just in case,
        // release all locks and close file
        $this->file->close();
    }

    public function file()
    {
        return $this->file;
    }

    public function getHandle()
    {
        return $this->file->getHandle();
    }

    public function getPath()
    {
        return $this->file->getPath();
    }

    public function exists()
    {
        return $this->file->exists();
    }

    public function drop()
    {
        return $this->file->drop();
    }

    public function acquireRead()
    {
        if ($this->lock !== LOCK_UN) {  /* Already have at least a read lock */
            ++$this->rcount;

            return true;
        } elseif ($this->lock === LOCK_UN) {/* New lock */
            if ($this->file->open('rb')) {
                $this->lock(LOCK_SH);
                $this->rcount = 1;

                return true;
            }
        }

        return false;
    }

    public function acquireWrite()
    {
        if ($this->lock === LOCK_EX) {/* Already have a write lock */
            ++$this->wcount;

            return true;
        } elseif ($this->lock === LOCK_SH) {/* Upgrade a lock*/
            $this->lock(LOCK_EX);
            ++$this->wcount;

            return true;
        } elseif ($this->lock === LOCK_UN) {/* New lock */
            if ($this->file->open('c+b')) {
                $this->lock(LOCK_EX);
                $this->wcount = 1;

                return true;
            }
        }

        return false;
    }

    public function releaseRead()
    {
        if ($this->lock !== LOCK_UN) {
            --$this->rcount;

            if ($this->lock === LOCK_SH && $this->rcount === 0) {/* Read lock now empty */
                // no readers or writers left, release lock
                $this->close();
            }
        }

        return true;
    }

    public function releaseWrite()
    {
        if ($this->lock !== LOCK_UN) {
            if ($this->lock === LOCK_EX) {/* Write lock */
                --$this->wcount;
                if ($this->wcount === 0) {
                    // no writers left.

                    if ($this->rcount > 0) {
                        // only readers left.  downgrade lock.
                        $this->lock(LOCK_SH);
                    } else {
                        // no readers or writers left, release lock
                        $this->close();
                    }
                }
            }
        }

        return true;
    }

    private function lock($mode)
    {
        $this->file->lock($mode);
        $this->lock = $mode;
    }

    private function close()
    {
        $this->file->close();
        $this->lock = LOCK_UN;
    }
}
