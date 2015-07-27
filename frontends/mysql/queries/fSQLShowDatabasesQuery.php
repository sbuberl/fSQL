<?php

class fSQLShowTablesQuery extends fSQLQuery
{	
	function execute()
	{
		$data = array();
		foreach(array_keys($this->environment->databases) as $db_name)
			$data[] = array($db_name);
			
		return $this->environment->_create_result_set(
					array(
						array('name'=>'Database','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null)
					),
					$data
				);
	}
}

?>