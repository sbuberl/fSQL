<?php

class fSQLUseQuery extends fSQLQuery
{	
	var $db_name;
	
	function fSQLUseQuery(&$environment, $db_name)
	{
		parent::fSQLQuery($environment);
		$this->db_name = $db_name;
	}
	
	function execute()
	{
		return $this->environment->select_db($this->db_name);
	}
}

?>