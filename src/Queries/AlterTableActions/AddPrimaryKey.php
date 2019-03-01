<?php

namespace FSQL\Queries\AlterTableActions;

use FSQL\Environment;

class AddPrimaryKey extends BaseAction
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
            return $this->environment->set_error("Column named '{$this->columnName}' does not exist in table '$tableName'");
        }

        foreach ($columns as $name => $column) {
            if ($column['key'] == 'p') {
                return $this->environment->set_error('Primary key already exists');
            }
        }

        $columns[$this->columnName]['key'] = 'p';
        $table->setColumns($columns);
        return true;
    }
}
