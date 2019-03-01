<?php

namespace FSQL\Queries;

use FSQL\Environment;

class DropSequence extends DropRelationBase
{
    public function execute()
    {
        $sequenceName = $this->fullName[2];

        $schema = $this->environment->find_schema($this->fullName[0], $this->fullName[1]);
        if ($schema === false) {
            return false;
        }

        $sequence = $this->getRelation($schema, $sequenceName);
        if ($sequence === true) {
            return true;
        } elseif ($sequence === false) {
            return $this->environment->error_relation_not_exists($this->fullName, 'Sequence');
        }
        $sequences = $schema->getSequences();
        return $sequences->dropSequence($sequenceName);
    }
}