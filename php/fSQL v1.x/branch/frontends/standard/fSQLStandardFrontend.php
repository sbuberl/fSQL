<?php

require FSQL_FRONTENDS_PATH.'/standard/fSQLStandardParser.php';

class fSQLStandardFrontend
{
	function &createFunctions(&$env)
	{
		fsql_load_class('fSQLStandardFunctions', FSQL_FRONTENDS_PATH.'/standard');
		$funcs =& new fSQLStandardFunctions($env);
		return $funcs;
	}
	
	function &createParser(&$env)
	{
		$parser =& new fSQLStandardParser($env);
		return $parser;
	}
}

?>