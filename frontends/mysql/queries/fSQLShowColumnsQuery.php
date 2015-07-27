<?php

class fSQLShowColumnsQuery extends fSQLQuery
{
	var $tableName;
	
	var $full;
	
	function fSQLShowColumnsQuery(&$environment, $tableName, $full)
	{
		parent::fSQLQuery($environment);
		$this->tableName = $tableName;
		$this->full = $full;
	}
	
	function execute()
	{
		$tableObj =& $this->environment->_find_table($this->tableName);
		if($tableObj === false)
			return false;
		
		$tableDef = $tableObj->getDefinition();
		$data = array();
			
		foreach($tableObj->getColumns() as $name => $column) {
			$type = $this->environment->_typecode_to_name($column['type']);
			$default = $column['default'];
			$null = ($column['null']) ? 'YES' : 'NO';
			$extra = ($column['auto']) ? 'auto_increment' : '';
			
			switch($column['key'])
			{
				case 'p':
					$key = 'PRI';
					break;
				case 'u':
					$key = 'UNI';
					break;
				default:
					$key = '';
					break;
			}

			if($this->full)
				$data[] = array($name, $type, null, $null, $default, $key, $extra,'select,insert,update,references','');
			else
				$data[] = array($name, $type, $null, $default, $key, $extra);
		}
		
		$columns = array(
						array('name'=>'Field','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null),
						array('name'=>'Type','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null),
						array('name'=>'Null','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null),
						array('name'=>'Default','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>true,'auto'=>'false','key'=>'n','restraint'=>null),
						array('name'=>'Key','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null),
						array('name'=>'Extra','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null)
					);
		if($this->full)
		{
			 array_splice($columns, 2, 0, array(
						array('name'=>'Correlation','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null)
					)
				);
			$columns[] = array('name'=>'Privileges','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null);
			$columns[] = array('name'=>'Comment','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null);
		}
			
		return $this->environment->_create_result_set($columns, $data);
	}
}

?>