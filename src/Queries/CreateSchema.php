<?php

namespace FSQL\Queries;

class CreateSchema extends CreateBase
{
    public function execute()
    {
        $db = $this->environment->get_database($this->fullName[0]);
        if ($db === false) {
            return false;
        }

        $schemaName = $this->fullName[1];
        $schema = $db->getSchema($schemaName);
        if ($schema !== false) {
            if (!$this->ifNotExists) {
                return $this->environment->set_error("Schema {$schema->fullName()} already exists");
            } else {
                return true;
            }
        }

        return $db->defineSchema($schemaName) !== false;
    }
}
