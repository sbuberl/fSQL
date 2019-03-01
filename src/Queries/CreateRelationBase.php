<?php

namespace FSQL\Queries;

use FSQL\Database\Schema;

abstract class CreateRelationBase extends CreateBase
{
    protected function getRelation(Schema $schema, $relationName)
    {
        $relation = $schema->getRelation($relationName);
        if ($relation !== false) {
            if (!$this->ifNotExists) {
                return $this->environment->set_error("Relation {$relation->fullName()} already exists");
            } else {
                return true;
            }
        }

        // Return anything not a boolean to continue on
        return 42;
    }
}
