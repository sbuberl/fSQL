<?php

class fSQLStartQuery extends fSQLQuery
{	
	function execute()
	{
		return $this->environment->_begin();
	}
}

?>