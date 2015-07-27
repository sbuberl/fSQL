<?php

class fSQLDropTableQuery extends fSQLQuery
{	
	var $tableNames;
	
	var $ifExists;
	
	var $isView;
	
	function fSQLDropTableQuery(&$environment, $tableNames, $ifExists, $isView)
	{
		parent::fSQLQuery($environment);
		$this->tableNames = $tableNames;
		$this->ifExists = $ifExists;
		$this->isView = $isView;
	}
	
	function execute()
	{
		foreach($this->tableNames as $tableName)
		{
			$schema =& $this->environment->_find_schema($tableName[0], $tableName[1]);
			if($schema === false)
				return false;
			$table =& $schema->getTable($tableName[2]);
			if($table !== false)
			{
				if($table->isReadLocked()) {
					return $this->environment->_error_table_read_lock($tableName);
				}

				$master =& $this->environment->_get_master_schema();
				$master->removeTable($table);
				if($schema->dropTable($table->getName()) === true)
					return true;
				else
					return false;
			}
			else if(!$this->ifExists) {
				return $this->environment->_error_table_not_exists($tableName); 
			}
		}
		
		return true;
	}
}

?>