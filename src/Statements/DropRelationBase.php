<?php

namespace FSQL\Statements;

use FSQL\Database\Schema;

abstract class DropRelationBase extends DropBase
{
    protected function getRelation(Schema $schema, $relationName)
    {
        $relation = $schema->getRelation($relationName);
        if ($relation === false) {
            return $this->ifExists;
        }

        return $relation;
    }
}
