<?php

class fSQLRollbackQuery extends fSQLQuery
{	
	function fSQLRollbackQuery(&$environment)
	{
		parent::fSQLQuery($environment);
	}
	
	function execute()
	{
		return $this->environment->_rollback();
	}
}

?>