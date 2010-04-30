<?php

require FSQL_FRONTENDS_PATH.'/mysql/fSQLMysqlParser.php';

class fSQLMysqlFrontend extends fSQLStandardFrontend
{	
	function &createFunctions(&$env)
	{
		fsql_load_class('fSQLMysqlFunctions', FSQL_FRONTENDS_PATH.'/mysql');
		$funcs =& new fSQLMysqlFunctions($env);
		return $funcs;
	}
	
	function &createParser(&$env)
	{
		$parser =& new fSQLMysqlParser($env);
		return $parser;
	}
	
	function getTypes()
	{
		static $types = null;
		if($types === null)
		{
			$types = array_merge(parent::getTypes(), array(
				'TINYTEXT' => FSQL_TYPE_STRING,
				'MEDIUMTEXT' => FSQL_TYPE_STRING,
				'LONGTEXT' => FSQL_TYPE_STRING,
				
				'SET' => FSQL_TYPE_STRING,
				
				'TINYBLOB' => FSQL_TYPE_STRING,
				'MEDIUMBLOB' => FSQL_TYPE_STRING,
				'LONGBLOB' => FSQL_TYPE_STRING,
				
				'TINYINT' => FSQL_TYPE_INTEGER,
				'MEDIUMINT' => FSQL_TYPE_INTEGER,
				
				'DATETIME' => FSQL_TYPE_TIMESTAMP,
				'YEAR' => FSQL_TYPE_INTEGER
			));
		}
		return $types;
	}
}

?>