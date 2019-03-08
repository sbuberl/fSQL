<?php

namespace FSQL;

use FSQL\Database\TableCursor;

class ResultSet
{
    const FETCH_ASSOC = 1;
    const FETCH_NUM = 2;
    const FETCH_BOTH = 3;

    private $columnNames;
    private $data;
    private $columnsIndex;
    private $dataCursor;

    public function __construct(array $columnNames, array $data)
    {
        $this->columnNames = $columnNames;
        $this->data = $data;
        $this->columnsIndex = 0;
        $this->dataCursor = new TableCursor($data);
    }

    public function createMetadata()
    {
        return new ResultSet($this->columnNames, []);
    }

    public function fetchAll($type = self::FETCH_ASSOC)
    {
        if ($type === self::FETCH_NUM) {
            return $this->data;
        }

        $columnNames = $this->columnNames;
        $result_array = array();
        if ($type === self::FETCH_ASSOC) {
            foreach ($this->data as $entry) {
                $result_array[] = array_combine($columnNames, $entry);
            }
        } else {
            foreach ($this->data as $entry) {
                $result_array[] = array_merge($entry, array_combine($columnNames, $entry));
            }
        }

        return $result_array;
    }

    public function fetchArray($type = self::FETCH_ASSOC)
    {
        if (!$this->dataCursor->valid()) {
            return null;
        }

        $entry = $this->dataCursor->current();
        $this->dataCursor->next();
        if ($type === self::FETCH_ASSOC) {
            return array_combine($this->columnNames, $entry);
        } elseif ($type === self::FETCH_NUM) {
            return $entry;
        } else {
            return array_merge($entry, array_combine($this->columnNames, $entry));
        }
    }

    public function fetchAssoc()
    {
        return $this->fetchArray(self::FETCH_ASSOC);
    }

    public function fetchRow()
    {
        return $this->fetchArray(self::FETCH_NUM);
    }

    public function fetchBoth()
    {
        return $this->fetchArray(self::FETCH_BOTH);
    }

    public function fetchSingle($column = 0)
    {
        $type = is_numeric($column) ? self::FETCH_NUM : self::FETCH_ASSOC;
        $row = $this->fetchArray($type);

        return $row !== null && array_key_exists($column, $row) ? $row[$column] : null;
    }

    public function fetchObject()
    {
        $row = $this->fetchAssoc();
        if ($row === null) {
            return null;
        }

        return (object) $row;
    }

    public function dataSeek($i)
    {
        return $this->dataCursor->seek($i);
    }

    public function numRows()
    {
        return $this->dataCursor->count();
    }

    public function numFields()
    {
        return count($this->columnNames);
    }

    public function fetchField()
    {
        $pos = $this->columnsIndex;
        if (!isset($this->columnNames[$pos])) {
            return false;
        }
        $field = new \stdClass();
        $field->name = $this->columnNames[$pos];
        ++$this->columnsIndex;

        return $field;
    }

    public function fieldSeek($i)
    {
        if (!isset($this->columnNames[$i])) {
            return false;
        }
        $this->columnsIndex = $i;
    }
}
