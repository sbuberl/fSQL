<?php

namespace FSQL\Database;

use FSQL\LockableFile;
use FSQL\MicrotimeLockFile;
use FSQL\TempFile;

class TempTable extends CachedTable
{
    public function __construct(Schema $schema, $tableName)
    {
        Table::__construct($schema, $tableName);
        $this->columnsLockFile = new MicrotimeLockFile(new TempFile());
        $this->columnsFile = new LockableFile(new TempFile());
        $this->dataLockFile = new MicrotimeLockFile(new TempFile());
        $this->dataFile = new LockableFile(new TempFile());
    }

    public function temporary()
    {
        return true;
    }
}
