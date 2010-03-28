<?php

class fSQLDropSchemaQuery extends fSQLQuery
{
	var $databaseName;
	
	var $schemaName;
	
	var $ifExists;
	
	function fSQLDropSchemaQuery(&$environment, $databaseName, $schemaName, $ifExists)
	{
		parent::fSQLQuery($environment);
		$this->databaseName = $databaseName;
		$this->schemaName = $schemaName;
		$this->ifExists = $ifExists;
	}
	
	function execute()
	{
		$schema =& $this->environment->_find_schema($this->databaseName, $this->schemaName);
		if($schema === false)
		{
			if(!$this->ifExists)
				return $this->environment->_set_error("Schema '{$this->schemaName}' does not exist"); 
			else
				return true;
		}
		
		$master =& $this->environment->_get_master_schema();
		$master->removeSchema($schema);
		if($db->dropSchema($this->schemaName) === true)
			return true;
		else
			return false;
	}
}

?>