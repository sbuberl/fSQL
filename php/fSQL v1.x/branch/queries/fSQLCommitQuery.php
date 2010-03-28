<?php

class fSQLCommitQuery extends fSQLQuery
{	
	function fSQLCommitQuery(&$environment)
	{
		parent::fSQLQuery($environment);
	}
	
	function query($query)
	{
		if(preg_match('/\ACOMMIT\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->environment->_commit();
			return true;
		} else {
			return $this->environment->_set_error('Invalid Query');
		}
	}
}

?>