<?php

namespace FSQL\Queries;

use FSQL\Environment;

class AlterTable extends Query
{
    private $tableName;
    private $actions;

    public function  __construct(Environment $environment, array $tableName, array $actions)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->actions = $actions;
    }

    public function execute()
    {
        foreach($this->actions as $action) {
            $passed = $action->execute();
            if (!$passed) {
                return $passed;
            }
        }
        return true;
    }
}