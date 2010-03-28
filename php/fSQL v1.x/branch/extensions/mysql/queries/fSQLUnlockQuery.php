<?php

class fSQLUnlockQuery extends fSQLQuery
{	
	function fSQLUnlockQuery(&$environment)
	{
		parent::fSQLQuery($environment);
	}
	
	function execute()
	{
		return $this->environment->_unlock_tables();
	}
}

?>