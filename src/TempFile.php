<?php

namespace FSQL;

class TempFile extends File
{
    public function __construct()
    {
        $handle = tmpfile();
        $metaDatas = stream_get_meta_data($handle);
        $tmpFilename = $metaDatas['uri'];
        parent::__construct($tmpFilename);
        $this->handle = $handle;
    }

    public function __destruct()
    {
        parent::close();
    }

    public function exists()
    {
        return true;
    }

    public function open($mode)
    {
        return true;
    }

    public function close()
    {
        if ($this->handle) {
            flock($this->handle, LOCK_UN);
        }
    }

    public function drop()
    {
        parent::close();

        return true;
    }
}
