<?php

class fSQLStartQuery extends fSQLQuery
{	
	function fSQLStartQuery(&$environment)
	{
		parent::fSQLQuery($environment);
	}
	
	function execute()
	{
		return $this->environment->_begin();
	}
}

?>