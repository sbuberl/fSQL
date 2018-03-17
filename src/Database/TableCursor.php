<?php

namespace FSQL\Database;

class TableCursor implements \SeekableIterator, \Countable
{
    protected $entries;
    protected $numRows;
    protected $currentRowId;

    public function __construct(array &$entries)
    {
        $this->entries = &$entries;
        $this->rewind();
    }

    public function count()
    {
        return $this->numRows;
    }

    public function rewind()
    {
        $this->numRows = count($this->entries);
        $this->updateRowId(reset($this->entries));
    }

    public function key()
    {
        return $this->currentRowId;
    }

    public function current()
    {
        return $this->currentRowId !== false ? $this->entries[$this->currentRowId] : false;
    }

    public function valid()
    {
        return $this->currentRowId !== false;
    }

    public function next()
    {
        $this->updateRowId(next($this->entries));
    }

    public function seek($pos)
    {
        if ($pos >= 0 && $pos < count($this->entries)) {
            reset($this->entries);
            for ($i = 0; $i < $pos; $i++, next($this->entries)) {
            }
            $this->currentRowId = key($this->entries);

            return $this->currentRowId;
        } else {
            return false;
        }
    }

    private function updateRowId($result)
    {
        $this->currentRowId = $result !== false ? key($this->entries) : false;
    }
}
