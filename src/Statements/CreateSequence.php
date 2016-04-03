<?php

namespace FSQL\Statements;

use FSQL\Environment;

class CreateSequence extends CreateRelationBase
{
    private $initialValues;

    public function __construct(Environment $environment, array $fullName, $ifNotExists, array $initialValues)
    {
        parent::__construct($environment, $fullName, $ifNotExists);
        $this->initialValues = $initialValues;
    }

    public function initialValues()
    {
        return $this->initialValues;
    }

    public function execute()
    {
        $sequenceName = $this->fullName[2];

        $schema = $this->environment->find_schema($this->fullName[0], $this->fullName[1]);
        if ($schema === false) {
            return false;
        }

        $sequence = $this->getRelation($schema, $sequenceName);
        if (is_bool($sequence)) {
            return $sequence;
        }

        list($start, $increment, $min, $max, $cycle) = $this->initialValues;

        $sequences = $schema->getSequences();
        if (!$sequences->exists()) {
            $sequences->create();
        }

        $sequences->addSequence($sequenceName, $start, $increment, $min, $max, $cycle);

        return true;
    }
}
