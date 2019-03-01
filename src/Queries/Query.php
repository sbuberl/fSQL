<?php

namespace FSQL\Queries;

use FSQL\Environment;

abstract class Query
{
    protected $environment;

    public function __construct(Environment $environment)
    {
        $this->environment = $environment;
    }

    abstract public function execute();
}
