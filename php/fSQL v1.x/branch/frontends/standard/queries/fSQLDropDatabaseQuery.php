<?php

class fSQLDropDatabaseQuery extends fSQLQuery
{
	var $databaseName;
	
	var $ifExists;
	
	function fSQLDropDatabaseQuery(&$environment, $databaseName, $ifExists)
	{
		parent::fSQLQuery($environment);
		$this->databaseName = $databaseName;
		$this->ifExists = $ifExists;
	}
	
	function execute()
	{
		$db_name = $this->databaseName;
		if(!$this->ifExists && !isset($this->environment->databases[$db_name])) {
			return $this->environment->_set_error("Database '{$db_name}' does not exist"); 
		} else if(!isset($this->environment->databases[$db_name])) {
			return true;
		}
			
		$db =& $this->environment->databases[$db_name];
		$master =& $this->environment->_get_master_schema();
		$master->removeDatabase($db);
		if($db->drop() === true)
		{
			unset($this->environment->databases[$db_name]);
			return true;
		}
		else
			return false;
	}
}

?>