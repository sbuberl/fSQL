<?php

namespace FSQL\Statements;

use FSQL\Environment;

class SetDatabase extends Statement
{
    private $dbName;

    public function  __construct(Environment $environment, $dbName)
    {
        parent:: __construct($environment);
        $this->dbName = $dbName;
    }

    public function execute()
    {
        return $this->environment->select_db($this->dbName);
    }
}
