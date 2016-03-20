<?php

namespace FSQL\Database;

class TableCursor
{
    private $entries;
    private $num_rows;
    private $pos;

    public function __construct(array &$entries)
    {
        $this->entries = &$entries;
        $this->first();
    }

    public function first()
    {
        $this->num_rows = count($this->entries);
        $this->pos = 0;

        return $this->pos;
    }

    public function getPosition()
    {
        return $this->pos;
    }

    public function getRow()
    {
        if ($this->pos >= 0 && $this->pos < $this->num_rows) {
            return $this->entries[$this->pos];
        } else {
            return false;
        }
    }

    public function isDone()
    {
        return $this->pos < 0 || $this->pos >= $this->num_rows;
    }

    public function last()
    {
        $this->pos = $this->num_rows - 1;
    }

    public function previous()
    {
        --$this->pos;
    }

    public function next()
    {
        ++$this->pos;

        return $this->pos;
    }

    public function seek($pos)
    {
        if ($pos >= 0 & $pos < count($this->entries)) {
            $this->pos = $pos;
        }
    }
}
