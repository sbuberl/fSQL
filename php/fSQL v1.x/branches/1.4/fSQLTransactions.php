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
		unset($this->environment);
		unset($this->updatedTables);
		unset($this->previousAutoValue);
	}
	
	function begin()
	{
		$this->previousAutoValue = $this->environment->auto;
		$this->environment->auto = 0;
		$this->environment->_unlock_tables();
		return true;
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
		$this->destroy();
		return true;
	}
	
	function markTableAsUpdated(&$table)
	{
		$table_fqn = $table->getFullName();
		if(!isset($this->updatedTables[$table_fqn]))
			$this->updatedTables[$table_fqn] =& $table;
		return true;
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
		$this->destroy();
		return true;
	}
}

?>