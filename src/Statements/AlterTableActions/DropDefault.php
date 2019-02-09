<?php

namespace FSQL\Statements\AlterTableActions;

use FSQL\Environment;

class DropDefault extends BaseAction
{
    private $tableName;
    private $columnName;
    private $default;

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
        $default = $this->environment->get_type_default_value($column['type'], $column['null']);

        $columns[$this->columnName]['default'] = $default;
        $table->setColumns($columns);
        return true;
    }
}
