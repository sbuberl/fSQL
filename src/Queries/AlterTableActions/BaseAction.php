<?php

namespace FSQL\Queries\AlterTableActions;

use FSQL\Environment;

abstract class BaseAction
{
    protected $environment;

    public function __construct(Environment $environment)
    {
        $this->environment = $environment;
    }

    abstract public function execute();
}
