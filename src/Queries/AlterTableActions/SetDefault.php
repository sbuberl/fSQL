<?php

namespace FSQL\Queries\AlterTableActions;

use FSQL\Environment;

class SetDefault extends BaseAction
{
    private $tableName;
    private $columnName;
    private $default;

    public function  __construct(Environment $environment, array $tableName, $columnName, $default)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->columnName = $columnName;
        $this->default = $default;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->tableName);
        if ($table === false) {
            return false;
        }

        $columns = $table->getColumns();
        $column = $columns[$this->columnName];
        $default = $this->environment->parser()->parseDefault($this->default, $column['type'], $column['null'], $column['restraint']);

        $columns[$this->columnName]['default'] = $default;
        $table->setColumns($columns);
        return true;
    }
}
