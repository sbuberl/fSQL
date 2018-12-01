<?php

namespace FSQL\Statements;

use FSQL\Environment;
use FSQL\ResultSet;

class ShowTables extends Statement
{
    private $dbName;
    private $schemaName;
    private $full;

    public function  __construct(Environment $environment, $dbName, $schemaName, $full)
    {
        parent:: __construct($environment);
        $this->dbName = $dbName;
        $this->schemaName = $schemaName;
        $this->full = $full;
    }

    public function execute()
    {
        $schema = $this->environment->find_schema($this->dbName, $this->schemaName);
        if ($schema === false) {
            return false;
        }

        $tables = $schema->listTables();
        $data = [];

        foreach ($tables as $tableName) {
            if ($this->full) {
                $data[] = [$tableName, 'BASE TABLE'];
            } else {
                $data[] = [$tableName];
            }
        }

        $columns = ['Name'];
        if ($this->full) {
            $columns[] = 'Table_type';
        }

        return new ResultSet($columns, $data);
    }
}