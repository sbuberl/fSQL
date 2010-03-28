<?php

class fSQLTransaction
{
	var $environment;
	
	var $updatedTables = array();
	
	var $previousAutoValue;
	
	function fSQLTransaction(&$environment)
	{
		$this->environment =& $environment;
	}
	
	function destroy()
	{
		$this->environment = null;
		$this->updatedTables = null;
	}
	
	function begin()
	{
		$this->previousAutoValue = $this->environment->auto;
		$this->environment->auto = 0;
		$this->environment->_unlock_tables();
	}
	
	function commit()
	{
		if(!empty($this->updatedTables))
		{
			foreach (array_keys($this->updatedTables) as $index ) {
				$this->updatedTables[$index]->commit();
			}
			$this->updatedTables = array();
		}
		$this->environment->auto = $this->previousAutoValue;
	}
	
	function rollback()
	{
		if(!empty($this->updatedTables))
		{
			foreach (array_keys($this->updatedTables) as $index ) {
				$this->updatedTables[$index]->rollback();
			}
			$this->updatedTables = array();
		}
		$this->environment->auto = $this->previousAutoValue;
	}
	
	function setTableAsUpdated(&$table)
	{
		$table_fqn = $table->getFullName();
		if(!isset($this->updatedTables[$table_fqn]))
			$this->updatedTables[$table_fqn] =& $table;
	}
}

?>