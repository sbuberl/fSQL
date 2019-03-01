<?php

namespace FSQL\Queries;

use FSQL\Environment;

abstract class CreateBase extends Query
{
    protected $fullName;

    protected $ifNotExists;

    public function __construct(Environment $environment, array $fullName, $ifNotExists)
    {
        parent::__construct($environment);
        $this->fullName = $fullName;
        $this->ifNotExists = $ifNotExists;
    }
}
