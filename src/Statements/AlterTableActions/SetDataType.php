<?php

namespace FSQL\Statements\AlterTableActions;

use FSQL\Environment;
use FSQL\Functions;

class SetDataType extends BaseAction
{
    private $tableName;
    private $columnName;
    private $type;

    public function  __construct(Environment $environment, array $tableName, $columnName, $type, Functions $functions)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->columnName = $columnName;
        $this->type = $type;
        $this->functions = $functions;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->tableName);
        if ($table === false) {
            return false;
        }

        $columns = $table->getColumns();
        $columnIndex = array_search($this->columnName, array_keys($columns));

        $columns[$this->columnName]['type'] = $this->type;
        $table->setColumns($columns);

        $cursor = $table->getWriteCursor();
        foreach($cursor as $entry) {
            $newValue = $this->functions->cast($entry[$columnIndex], $this->type);
            $cursor->updateRow([$columnIndex => $newValue]);
        }
        $table->commit();

        return true;
    }
}
