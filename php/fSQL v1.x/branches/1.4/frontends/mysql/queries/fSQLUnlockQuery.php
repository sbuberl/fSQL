<?php

class fSQLUnlockQuery extends fSQLQuery
{	
	function execute()
	{
		return $this->environment->_unlock_tables();
	}
}

?>