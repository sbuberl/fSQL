<?php

namespace FSQL\Queries\AlterTableActions;

use FSQL\Environment;

class AddColumn extends BaseAction
{
    private $tableName;
    private $columnName;
    private $type;
    private $auto;
    private $default;
    private $key;
    private $null;
    private $restraint;

    public function  __construct(Environment $environment, array $tableName, $columnName, $type, $auto, $default, $key, $null, $restraint)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->columnName = $columnName;
        $this->type = $type;
        $this->auto = $auto;
        $this->default = $default;
        $this->key = $key;
        $this->null = $null;
        $this->restraint = $restraint;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->tableName);
        if ($table === false) {
            return false;
        }

        $columns = $table->getColumns();
        $columns[$this->columnName] = ['type' => $this->type, 'auto' => $this->auto, 'default' => $this->default, 'key' => $this->key, 'null' => $this->null, 'restraint' => $this->restraint];
        $table->setColumns($columns);

        $cursor = $table->getWriteCursor();
        $update = [count($columns) - 1 => $this->default];
        foreach($cursor as $entry) {
            $cursor->updateRow($update);
        }
        $table->commit();
        return true;
    }
}
