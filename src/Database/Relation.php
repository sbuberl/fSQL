<?php

namespace FSQL\Database;

interface Relation
{
    public function name();

    public function drop();
}
