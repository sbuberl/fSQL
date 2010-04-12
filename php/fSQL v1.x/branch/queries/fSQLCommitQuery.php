<?php

class fSQLCommitQuery extends fSQLQuery
{	
	function execute()
	{
		return $this->environment->_commit();
	}
}

?>