<?php

class fSQLRenameQuery extends fSQLQuery
{	
	var $renames = null;
	
	function fSQLRenameQuery(&$environment, $renames)
	{
		parent::fSQLQuery($environment);
		$this->renames = $renames;
	}
	
	function execute()
	{
		foreach($this->renames as $rename)
		{
			$oldTableNamePieces = $rename['old'];
			$newTableNamePieces = $rename['new'];
			
			$old_table =& $this->environment->_find_table($oldTableNamePieces);
			if($old_table === false)
				return false;
			else if($old_table->isReadLocked())
				return $this->environment->_error_table_read_lock($oldTableNamePieces);
			
			
			$new_schema =& $this->_find_schema($newTableNamePieces[0], $newTableNamePieces[1]);
			if($new_schema === false)
				return false;
			
			$new_table_name = $newTableNamePieces[2];
			$new_table =& $new_schema->getTable($new_table_name);
			if($new_table === false) {
				$old_schema =& $old_table->getSchema();
				return $old_schema->renameTable($old_table->getName(), $new_table_name, $new_schema);
			} else {
				return $this->_set_error("Destination table {${new_table->getFullName()}} already exists");
			}
		}
	}
}

?>