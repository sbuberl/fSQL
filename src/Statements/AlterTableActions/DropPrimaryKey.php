<?php

namespace FSQL\Statements\AlterTableActions;

use FSQL\Environment;

class DropPrimaryKey extends BaseAction
{
    private $tableName;

    public function  __construct(Environment $environment, array $tableName)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->tableName);
        if ($table === false) {
            return false;
        }

        $columns = $table->getColumns();
        $found = false;
        foreach ($columns as $name => $column) {
            if ($column['key'] == 'p') {
                $columns[$name]['key'] = 'n';
                $found = true;
            }
        }

        if ($found) {
            $table->setColumns($columns);

            return true;
        } else {
            return $this->environment->set_error('No primary key found');
        }
    }
}
