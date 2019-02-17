<?php

namespace FSQL\Statements;

use FSQL\Environment;

class Unlock extends Statement
{
    public function  __construct(Environment $environment)
    {
        parent:: __construct($environment);
    }

    public function execute()
    {
        return $this->environment->unlock_tables();
    }
}
