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
}

?>