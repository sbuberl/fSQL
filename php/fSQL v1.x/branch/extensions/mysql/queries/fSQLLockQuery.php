<?php

class fSQLLockQuery extends fSQLQuery
{
	var $readTables;
	
	var $writeTables;
	
	function fSQLLockQuery(&$environment, $readTables, $writeTables)
	{
		parent::fSQLQuery($environment);
		$this->readTables = $readTables;
		$this->writeTables = $writeTables;
	}
	
	function execute()
	{	
		foreach($this->readTables as $tableName) {
			$table =& $this->environment->_find_table($tableName);
			$table->readLock();
			$this->environment->lockedTables[] =& $table;
		}
		
		foreach($this->writeTables as $tableName) {
			$table =& $this->environment->_find_table($tableName);
			$table->writeLock();
			$this->environment->lockedTables[] =& $table;
		}
		
		return true;
	}
}

?>