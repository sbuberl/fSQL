<?php

namespace FSQL\Statements;

use FSQL\Environment;

abstract class Statement
{
    protected $environment;

    public function __construct(Environment $environment)
    {
        $this->environment = $environment;
    }

    abstract public function execute();
}
