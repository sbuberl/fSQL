<?php

namespace FSQL\Queries;

use FSQL\Environment;

class Begin extends Query
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
