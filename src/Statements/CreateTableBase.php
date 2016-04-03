<?php

namespace FSQL\Statements;

use FSQL\Environment;

abstract class CreateTableBase extends CreateRelationBase
{
    protected $temporary;

    public function __construct(Environment $environment, array $fullName, $ifNotExists, $temporary)
    {
        parent::__construct($environment, $fullName, $ifNotExists);
        $this->temporary = $temporary;
    }

    public function temporary()
    {
        return $this->temporary;
    }
}
