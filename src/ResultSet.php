<?php

namespace FSQL;

use FSQL\Database\TableCursor;

class ResultSet
{
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

    public function fetchAll($type = FSQL_ASSOC)
    {
        if ($type === FSQL_NUM) {
            return $this->data;
        }

        $columnNames = $this->columnNames;
        $result_array = array();
        if ($type === FSQL_ASSOC) {
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

    public function fetchArray($type = FSQL_ASSOC)
    {
        if (!$this->dataCursor->valid()) {
            return false;
        }

        $entry = $this->dataCursor->current();
        $this->dataCursor->next();
        if ($type === FSQL_ASSOC) {
            return array_combine($this->columnNames, $entry);
        } elseif ($type === FSQL_NUM) {
            return $entry;
        } else {
            return array_merge($entry, array_combine($this->columnNames, $entry));
        }
    }

    public function fetchAssoc()
    {
        return $this->fetchArray(FSQL_ASSOC);
    }

    public function fetchRow()
    {
        return $this->fetchArray(FSQL_NUM);
    }

    public function fetchBoth()
    {
        return $this->fetchArray(FSQL_BOTH);
    }

    public function fetchSingle($column = 0)
    {
        $type = is_numeric($column) ? FSQL_NUM : FSQL_ASSOC;
        $row = $this->fetchArray($type);

        return $row !== false && array_key_exists($column, $row) ? $row[$column] : false;
    }

    public function fetchObject()
    {
        $row = $this->fetchAssoc();
        if ($row === false) {
            return false;
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
