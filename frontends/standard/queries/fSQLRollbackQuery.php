<?php

class fSQLRollbackQuery extends fSQLQuery
{	
	function execute()
	{
		return $this->environment->_rollback();
	}
}

?>