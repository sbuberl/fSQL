<?php

namespace FSQL\Database;

use FSQL\Functions;

abstract class Table implements Relation
{
    protected $name;
    protected $schema;
    protected $cursor = null;
    protected $writeCursor= null;
    protected $columns = null;
    protected $entries = null;
    protected $identity = null;
    protected $keys = [];

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

    abstract public function createKey($name, $type, $columns);

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

    public function getWriteCursor()
    {
        if ($this->writeCursor === null) {
            $this->writeCursor = new WriteCursor($this->entries, $this);
        }

        $this->writeCursor->rewind();

        return $this->writeCursor;
    }
    public function getIdentity()
    {
        if ($this->identity === null) {
            $colIndex = 0;
            foreach ($this->getColumns() as $columnName => $column) {
                if ($column['auto']) {
                    if (empty($column['restraint'])) {  // upgrade old AUTOINCREMENT column to IDENTITY
                        $this->upgradeAuto($colIndex, $columnName);
                    }

                    $this->identity = new Identity($this, $columnName);
                    $this->identity->load();
                    break;
                }
                ++$colIndex;
            }
        }

        return $this->identity;
    }

    private function upgradeAuto($colIndex, $columnName)
    {
        $always = false;
        $increment = 1;
        $min = 1;
        $max = PHP_INT_MAX;
        $cycle = false;

        $entries = $this->getEntries();

        $functions = new Functions($this->schema->database()->environment());
        $maxFunc = [$functions, 'max'];

        $largest = $maxFunc($entries, $colIndex, '');
        if ($max !== null) {
            $insert_id = $largest + 1;
        } else {
            $insert_id = 1;
        }

        $tableColumns = $this->getColumns();
        $tableColumns[$columnName]['restraint'] = array($insert_id, $always, $min, $increment, $min, $max, $cycle);
        $this->setColumns($tableColumns);

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

    public function getKeys()
    {
        return $this->keys;
    }

    abstract public function commit();

    abstract public function rollback();

    abstract public function isReadLocked();

    abstract public function readLock();

    abstract public function writeLock();

    abstract public function unlock();
}
