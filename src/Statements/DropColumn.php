<?php

namespace FSQL\Statements;

use FSQL\Environment;

class DropColumn extends Statement
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
        $tableName = $table->name();
        $columns = $table->getColumns();
        if (!isset($columns[$this->columnName])) {
            return $this->environment->set_error("Column named {$this->columnName} does not exist in table $tableName");
        }

        $table->dropColumn($this->columnName);
        return true;
    }
}
