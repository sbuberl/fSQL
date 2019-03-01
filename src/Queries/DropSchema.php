<?php

namespace FSQL\Queries;

use FSQL\Environment;

class DropSchema extends DropBase
{
    public function execute()
    {
        list($dbName, $schemaName) = $this->fullName;
        $schema = $this->environment->find_schema($dbName, $schemaName);
        if ($schema == false) {
            return $this->ifExists;
        }

        $database = $schema->database();
        return $database->dropSchema($schemaName);
    }
}