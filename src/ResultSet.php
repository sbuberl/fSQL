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

    public function create_metadata()
    {
        return new ResultSet($this->columnNames, []);
    }

    public function fetch_all($type = self::FETCH_ASSOC)
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

    public function fetch_array($type = self::FETCH_ASSOC)
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

    public function fetch_assoc()
    {
        return $this->fetch_array(self::FETCH_ASSOC);
    }

    public function fetch_row()
    {
        return $this->fetch_array(self::FETCH_NUM);
    }

    public function fetch_both()
    {
        return $this->fetch_array(self::FETCH_BOTH);
    }

    public function fetchSingle($column = 0)
    {
        $type = is_numeric($column) ? self::FETCH_NUM : self::FETCH_ASSOC;
        $row = $this->fetch_array($type);

        return $row !== null && array_key_exists($column, $row) ? $row[$column] : null;
    }

    public function fetch_object()
    {
        $row = $this->fetch_assoc();
        if ($row === null) {
            return null;
        }

        return (object) $row;
    }

    public function data_seek($i)
    {
        return $this->dataCursor->seek($i);
    }

    public function num_rows()
    {
        return $this->dataCursor->count();
    }

    public function field_count()
    {
        return count($this->columnNames);
    }

    public function current_field()
    {
        return $this->columnsIndex;
    }

    public function fetch_field()
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

    public function fetch_field_direct($fieldPos)
    {
        if (!isset($this->columnNames[$fieldPos])) {
            return false;
        }
        $field = new \stdClass();
        $field->name = $this->columnNames[$fieldPos];
        return $field;
    }

    public function fetch_fields()
    {
        $fields = [];
        foreach ($this->columnNames as $fieldName) {
            $field = new \stdClass();
            $field->name = $fieldName;
            $fields[] = $field;
        }
        return $fields;
    }

    public function field_seek($i)
    {
        if (!isset($this->columnNames[$i])) {
            return false;
        }
        $this->columnsIndex = $i;
    }
}
