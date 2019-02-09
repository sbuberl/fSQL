<?php

namespace FSQL\Statements\AlterTableActions;

use FSQL\Environment;

class AlterIdentity extends BaseAction
{
    private $tableName;
    private $columnName;
    private $options;

    public function  __construct(Environment $environment, array $tableName, $columnName, $options)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->columnName = $columnName;
        $this->options = $options;
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

        $identity = $table->getIdentity();
        $result = $identity->alter($this->options);
        if ($result !== true) {
            $identity->load();  // refresh temp changes made
            return $this->environment->set_error($result);
        }

        return true;
    }
}
