<?php

namespace FSQL\Database;

use FSQL\Environment;

class Transaction
{
    private $environment;

    private $updatedTables = [];

    private $previousAutoValue;

    function fSQLTransaction(Environment $environment)
    {
        $this->environment = $environment;
    }

    function begin()
    {
        $this->previousAutoValue = $this->environment->is_auto_commit();
        $this->environment->auto_commit(false);
        return true;
    }

    function commit()
    {
        if(!empty($this->updatedTables)) {
            foreach (array_keys($this->updatedTables) as $index ) {
                $this->updatedTables[$index]->commit();
            }
            $this->updatedTables = [];
        }
        $this->environment->auto_commit($this->previousAutoValue);
        return true;
    }

    function markTableAsUpdated(&$table)
    {
        $table_fqn = $table->getFullName();
        if(!isset($this->updatedTables[$table_fqn]))
            $this->updatedTables[$table_fqn] = $table;
        return true;
    }

    function rollback()
    {
        if(!empty($this->updatedTables)){
            foreach (array_keys($this->updatedTables) as $index ) {
                $this->updatedTables[$index]->rollback();
            }
            $this->updatedTables = [];
        }
        $this->environment->auto_commit($this->previousAutoValue);
        return true;
    }
}
