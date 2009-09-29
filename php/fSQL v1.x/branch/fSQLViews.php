<?php

class fSQLView extends fSQLTable
{
	var $query;
	var $columns = null;
	var $entries = null;
	
	function fSQLView($name, &$schema, $query, $columns = null)
	{
		parent::fSQLTable($name, $schema);
		$this->query = $query;
		$this->columns = $columns;
	}
	
	function close()
	{
		parent::close();
		unset($this->query);
		unset($this->columns);
	}
	
	function getQuery()
	{
		return $this->query;
	}
	
	function execute()
	{
		$env =& $this->schema->getDatabase()->getEnvironment();
		$rs_id = $env->query($this->query);
		$rs =& $env->get_result_set($rs_id);
		if($rs !== false)
		{
			if($this->columns === null)
				$this->columns = $rs->columns;
			$this->entries = $rs->data;
			$env->free_result($rs_id);
		}
		else
			return false;
	}
}

class fSQLTemporaryView extends fSQLView
{
	var $rcursor = null;
	
	function fSQLTemporaryView($name, &$schema, $query, $columns = null)
	{
		parent::fSQLView($name, $schema, $query, $columns);
	}
	
	function create($columns)
	{
		$this->execute();
		$this->schema->getDatabase()->getEnvironment()->_get_master_schema()->addTable($this);
	}
	
	function drop()
	{
		$this->schema->getDatabase()->getEnvironment()->_get_master_schema()->removeTable($this);
		$this->close();
	}
	
	function temporary()
	{
		return true;
	}
	
	function exists()
	{
		return true;
	}
	
	function getColumnNames() {
		return array_keys($this->getColumns());
	}
	
	function getColumns() {
		return $this->columns;
	}
	
	function setColumns($columns) {
		$this->columns = $columns;
	}
	
	function getEntries() {
		return $this->entries;
	}
	
	function &getCursor()
	{
		if($this->rcursor === NULL)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}
}

?>
