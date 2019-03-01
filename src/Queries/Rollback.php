<?php

namespace FSQL\Queries;

use FSQL\Environment;

class Rollback extends Query
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
