<?php

namespace FSQL\Statements;

use FSQL\Environment;

abstract class Statement
{
    protected $environment;

    protected function __construct(Environment $environment)
    {
        $this->environment = $environment;
    }

    abstract public function execute();
}
