<?php

namespace FSQL\Statements;

use FSQL\Environment;
use FSQL\ResultSet;

class ShowDatabases extends Statement
{
    public function execute()
    {
        $data = [];
        foreach ($this->environment->list_dbs() as $db) {
            $data[] = [$db];
        }

        $columns = ['Name'];

        return new ResultSet($columns, $data);
    }
}