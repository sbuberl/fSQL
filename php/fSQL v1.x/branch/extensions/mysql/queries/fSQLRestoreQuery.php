<?php

class fSQLRestoreQuery extends fSQLQuery
{
	var $originalPath;
	
	var $tableNames;
	
	function fSQLRestoreQuery(&$environment, $originalPath, $tableNames)
	{
		parent::fSQLQuery($environment);
		if(substr($originalPath, -1) !== '/')
			$originalPath .= '/';
		$this->originalPath = $originalPath;
		$this->tableNames = $tableNames;
	}
	
	function execute()
	{
		foreach($this->tableNames as $table_name_pieces) {
			$schema =& $this->_find_schema($table_name_pieces[0], $table_name_pieces[1]);
			if($schema === false)
				return false;
				
			$schema->copyTable($table_name_pieces[2], $this->originalPath, $db->getPath());
		}
		
		return true;
	}
}

?>