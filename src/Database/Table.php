<?php

namespace FSQL\Database;

abstract class Table implements Relation
{
    protected $name;
    protected $schema;
    protected $cursor = null;
    protected $columns = null;
    protected $entries = null;
    protected $identity = null;

    public function __construct(Schema $schema, $name)
    {
        $this->name = $name;
        $this->schema = $schema;
    }

    public function name()
    {
        return $this->name;
    }

    public function fullName()
    {
        return $this->schema->fullName().'.'.$this->name;
    }

    public function schema()
    {
        return $this->schema;
    }

    abstract public function exists();

    abstract public function temporary();

    abstract public function truncate();

    public function getColumnNames()
    {
        return array_keys($this->getColumns());
    }

    public function getColumns()
    {
        return $this->columns;
    }

    public function setColumns(array $columns)
    {
        $this->columns = $columns;
    }

    public function getEntries()
    {
        return $this->entries;
    }

    public function getCursor()
    {
        if ($this->cursor === null) {
            $this->cursor = new TableCursor($this->entries);
        }

        $this->cursor->rewind();

        return $this->cursor;
    }

    public function newCursor()
    {
        return new TableCursor($this->entries);
    }

    public function getIdentity()
    {
        if ($this->identity === null) {
            foreach ($this->getColumns() as $columnName => $column) {
                if ($column['auto']) {
                    $this->identity = new Identity($this, $columnName);
                    $this->identity->load();
                    break;
                }
            }
        }

        return $this->identity;
    }

    public function dropIdentity()
    {
        $this->getIdentity();
        if ($this->identity !== null) {
            $columns = $this->getColumns();
            $columnName = $this->identity->getColumnName();
            $columns[$columnName]['auto'] = '0';
            $columns[$columnName]['restraint'] = array();
            $this->identity = null;
            $this->setColumns($columns);
        }
    }

    public function insertRow(array $data)
    {
        $this->entries[] = $data;
    }

    public function updateRow($row, array $data)
    {
        foreach ($data as $key => $value) {
            $this->entries[$row][$key] = $value;
        }
    }

    public function deleteRow($row)
    {
        unset($this->entries[$row]);
    }

    abstract public function commit();

    abstract public function rollback();

    abstract public function isReadLocked();

    abstract public function readLock();

    abstract public function writeLock();

    abstract public function unlock();
}
