<?php

class fSQLBackupQuery extends fSQLQuery
{
	var $savePath;
	
	var $tableNames;
	
	function fSQLBackupQuery(&$environment, $savePath, $tableNames)
	{
		parent::fSQLQuery($environment);
		if(substr($savePath, -1) !== '/')
			$savePath .= '/';
		$this->savePath = $savePath;
		$this->tableNames = $tableNames;
	}
	
	function execute()
	{
		foreach($this->tableNames as $table_name_pieces) {
			$schema =& $this->_find_schema($table_name_pieces[0], $table_name_pieces[1]);
			if($schema === false)
				return false;
				
			$schema->copyTable($table_name_pieces[2], $db->getPath(), $this->savePath);
		}
		
		return true;
	}
}

?>