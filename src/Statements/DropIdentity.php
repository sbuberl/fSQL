<?php

namespace FSQL\Statements;

use FSQL\Environment;

class DropIdentity extends Statement
{
    private $tableName;
    private $columnName;

    public function  __construct(Environment $environment, array $tableName, $columnName)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->columnName = $columnName;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->tableName);
        if ($table === false) {
            return false;
        }

        $columns = $table->getColumns();
        $column = $columns[$this->columnName];
        if (!$column['auto']) {
            return $this->environment->set_error("Column {$this->columnName} is not an identity column");
        }

        $table->dropIdentity();
        return true;
    }
}
