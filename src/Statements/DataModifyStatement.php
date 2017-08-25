<?php

namespace FSQL\Statements;

use FSQL\Database\Table;
use FSQL\Environment;

abstract class DataModifyStatement extends Statement
{
    protected $affected = 0;

    public function affectedRows()
    {
        return $this->affected;
    }

    protected function commit(Table $table)
    {
        // if auto-commit, tell the table to commit and be done with it
        if($this->environment->is_auto_commit())
            return $table->commit();

        // get current transaction.  If one exists, mark the table
        // as updated.  otherwise, return error about missing transaction.
        $transaction = $this->environment->get_transaction();
        if($transaction !== null)
            return $transaction->markTableAsUpdated($table);
        else
            return $this->environment->set_error('Can not save changes because no transaction started');
    }
}
