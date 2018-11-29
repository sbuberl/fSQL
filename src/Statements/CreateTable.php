<?php

namespace FSQL\Statements;

use FSQL\Environment;

class CreateTable extends CreateTableBase
{
    private $columns;

    public function __construct(Environment $environment, array $fullName, $ifNotExists, $temporary, array $columns)
    {
        parent::__construct($environment, $fullName, $ifNotExists, $temporary);
        $this->columns = $columns;
    }

    public function execute()
    {
        $tableName = $this->fullName[2];

        $schema = $this->environment->find_schema($this->fullName[0], $this->fullName[1]);
        if ($schema === false) {
            return false;
        }

        $table = $this->getRelation($schema, $tableName);
        if (is_bool($table)) {
            return $table;
        }

        $schema->createTable($tableName, $this->columns, $this->temporary);

        return true;
    }
}
