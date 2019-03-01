<?php

namespace FSQL\Queries;

use FSQL\Environment;

abstract class DropBase extends Query
{
    protected $fullName;

    protected $ifExists;

    public function __construct(Environment $environment, array $fullName, $ifExists)
    {
        parent::__construct($environment);
        $this->fullName = $fullName;
        $this->ifExists = $ifExists;
    }
}
