<?php

namespace FSQL\Queries;

use FSQL\Environment;

class DropDatabase extends DropBase
{
    public function execute()
    {
        $dbName = $this->fullName[0];
        $database = $this->environment->get_database($dbName);
        if ($database === false) {
            return $this->ifExists;
        }
        return $this->environment->drop_database($dbName);
    }
}