<?php

class fSQLCreateViewQuery extends fSQLQuery
{
	var $viewNamePieces = null;
	
	var $columns = null;
	
	var $query = null;
	
	var $replace = false;
	
	function fSQLCreateViewQuery(&$environment, $viewNamePieces, $columns, $query, $replace)
	{
		parent::fSQLQuery($environment);
		$this->viewNamePieces = $viewNamePieces;
		$this->columns = null;
		$this->query = $query;
		$this->replace = $replace;
	}
	
	function execute()
	{
		$view_name = $this->viewNamePieces[2];
		$schema =& $this->environment->_find_schema($this->viewNamePieces[0], $this->viewNamePieces[1]);
		if($schema === false)
			return false;
		else if($schema->getTable($view_name) !== false)
		{
			if($this->replace)
				$schema->dropTable($view_name);
			else
				return $this->environment->_set_error("A relation named {$view_name} already exists");
		}
		
		$view =& $schema->createView($view_name, $this->query, $this->columns);
		if($view !== false)
		{
			$master =& $this->environment->_get_master_schema();
			$master->addTable($view);
			return true;
		}
		else
			return false;
	}
}

?>