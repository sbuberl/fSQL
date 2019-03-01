<?php

namespace FSQL\Queries;

use FSQL\Environment;

class AlterSequence extends Query
{
    private $fullName;
    private $ifExists;
    private $newValues;

    public function __construct(Environment $environment, array $fullName, $ifExists, $newValues)
    {
        parent::__construct($environment);
        $this->fullName = $fullName;
        $this->ifExists = $ifExists;
        $this->newValues = $newValues;
    }

    public function execute()
    {
        $schema = $this->environment->find_schema($this->fullName[0], $this->fullName[1]);
        if($schema === false) {
            return false;
        }

        $sequence = $this->environment->find_sequence($this->fullName);
        if ($sequence === false) {
            if (!$this->ifExists) {
                return $this->environment->error_relation_not_exists($this->fullName, 'Sequence');
            } else {
                return true;
            }
        }

        $parsed = $this->environment->parse_sequence_options($this->newValues, true);
        if ($parsed === false) {
            return false;
        }

        $sequences = $schema->getSequences();
        if (!$sequences->exists()) {
            $sequences->create();
        }

        $result = $sequence->alter($parsed);
        if ($result !== true) {
            $sequence->load();  // refresh temp changes made
            return $this->environment->set_error($result);
        }

        return true;
    }
}