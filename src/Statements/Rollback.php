<?php

namespace FSQL\Statements;

use FSQL\Environment;

class Rollback extends Statement
{
    public function  __construct(Environment $environment)
    {
        parent:: __construct($environment);
    }

    public function execute()
    {
        return $this->environment->rollback();
    }
}
