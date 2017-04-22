<?php

namespace FSQL\Database;

use FSQL\Environment;

class Transaction
{
    private $environment;

    private $updatedTables = [];

    private $previousAutoValue;

    public function __construct(Environment $environment)
    {
        $this->environment = $environment;
    }

    public function begin()
    {
        $this->previousAutoValue = $this->environment->is_auto_commit();
        $this->environment->auto_commit(false);
        return true;
    }

    private function finish(callable $operation)
    {
        if(!empty($this->updatedTables)){
            foreach (array_keys($this->updatedTables) as $index) {
                $operation($this->updatedTables[$index]);
            }
            $this->updatedTables = [];
        }
        $this->environment->auto_commit($this->previousAutoValue);
        return true;
    }

    public function commit()
    {
        return $this->finish(function (Table $table) {
            return $table->commit();
        });
    }

    public function markTableAsUpdated(Table $table)
    {
        $table_fqn = $table->fullName();
        if(!isset($this->updatedTables[$table_fqn]))
            $this->updatedTables[$table_fqn] = $table;
        return true;
    }

    public function rollback()
    {
        return $this->finish(function (Table $table) {
            return $table->rollback();
        });
    }
}
