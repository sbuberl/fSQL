<?php

class fSQLQuery
{	
	var $environment = null;
	
	function fSQLQuery(&$environment)
	{
		$this->environment =& $environment;
	}
	
	function prepare() { return true; }
	
	function execute() { return true; }
}

class fSQLDMLQuery extends fSQLQuery
{	
	var $affected = 0;
	
	function commit(&$table)
	{
		// if auto-commit, tell the table to commit and be done with it
		if($this->environment->auto)
			return $table->commit();
		
		// get current transaction.  If one exists, mark the table
		// as updated.  otherwise, return error about missing transaction.
		$transaction =& $this->environment->transaction;
		if($transaction !== null)
			return $transaction->markTableAsUpdated($table);
		else
			return $this->environment->_set_error('Can not save changes because no transaction started');
	}
}

?>
