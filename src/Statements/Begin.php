<?php

namespace FSQL\Statements;

use FSQL\Environment;

class Begin extends Statement
{
    public function  __construct(Environment $environment)
    {
        parent:: __construct($environment);
    }

    public function execute()
    {
        return $this->environment->begin();
    }
}
