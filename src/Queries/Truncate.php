<?php

namespace FSQL\Queries;

use FSQL\Environment;

class Truncate extends Query
{
    private $tableName;
    private $restart;

    public function  __construct(Environment $environment, array $tableName, $restart)
    {
        parent:: __construct($environment);
        $this->tableName = $tableName;
        $this->restart = $restart;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->tableName);
        if ($table === false) {
            return false;
        } elseif ($table->isReadLocked()) {
            return $this->error_table_read_lock($this->tableName);
        }

        $table->truncate();
        if ($this->restart) {
            $identity = $table->getIdentity();
            if ($identity !== null) {
                $identity->restart();
            }
        }

        return true;
    }
}