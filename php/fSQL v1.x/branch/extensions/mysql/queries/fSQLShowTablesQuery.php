<?php

class fSQLShowTablesQuery extends fSQLQuery
{	
	var $schemaName;
	
	var $full;
	
	var $orderby;
	
	function fSQLShowTablesQuery(&$environment, $schemaName, $full, $orderby)
	{
		parent::fSQLQuery($environment);
		$this->schemaName = $schemaName;
		$this->full = $full;
		$this->orderby = $orderby;
	}
	
	function execute()
	{
		if($this->schemaName)
		{
			$schema_name_pieces = $this->environment->_parse_schema_name($this->schemaName);
			if($schema_name_pieces !== false) {
				$schema =& $this->environment->_find_schema($schema_name_pieces[0], $schema_name_pieces[1]);
			}
			else
				return false;
		} else {
			$schema =& $this->environment->currentSchema;
		}
		
		if($schema === false)
			return false;
		
		$database =& $schema->getDatabase();
		$tables = $schema->listTables();
		$data = array();
		
		$columns = array(
						array('name' => 'Tables_in_'.$database->getName().'_'.$schema->getName(),'type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null)
					);
		
		if($this->full) {
			$columns[] = array('name'=>'Table_type','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null);
			foreach($tables as $table_name) {
				$table =& $schema->getTable($table_name);
				$table_type = !is_a($table, 'fSQLView') ? 'BASE TABLE' : 'VIEW';
				$data[] = array($table_name, $table_type);
			}
		}
		else
		{
			foreach($tables as $table_name)
				$data[] = array($table_name);
		}
		
		if($this->orderby !== null) {
			$this->orderby->sort($data);
		}
		
		return $this->_create_result_set($columns, $data);
	}
}

?>