<?php

class fSQLQuery
{	
	var $environment = null;
	
	function fSQLQuery(&$environment)
	{
		$this->environment =& $environment;
	}
	
	function prepare() { return true; }
	
	function execute() { return true; }
}

class fSQLDMLQuery extends fSQLQuery
{	
	var $affected = 0;
	
	function fSQLDMLQuery(&$environment)
	{
		parent::fSQLQuery($environment);
	}
}

?>
