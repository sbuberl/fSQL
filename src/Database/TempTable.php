<?php

namespace FSQL\Database;

class TempTable extends Table
{
    public function __construct(Schema $schema, $tableName, array $columnDefs)
    {
        parent::__construct($schema, $tableName);
        $this->columns = $columnDefs;
        $this->entries = array();
    }

    public function exists()
    {
        return true;
    }

    public function temporary()
    {
        return true;
    }

    public function drop()
    {
    }

    public function truncate()
    {
        $this->entries = array();
    }

    /* Unecessary for temporary tables */
    public function commit()
    {
    }
    public function rollback()
    {
    }
    public function isReadLocked()
    {
        return false;
    }
    public function readLock()
    {
    }
    public function writeLock()
    {
    }
    public function unlock()
    {
    }
}
