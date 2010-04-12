<?php

class fSQLCreateTableQuery extends fSQLQuery
{
	var $fullTableName;
	
	var $ifNotExists;
	
	var $temporary;
	
	var $columns;
	
	var $constraints;
	
	function fSQLCreateTableQuery(&$environment, $fullTableName, $columns, $constraints, $ifNotExists, $temporary)
	{
		parent::fSQLQuery($environment);
		$this->fullTableName = $fullTableName;
		$this->columns = $columns;
		$this->constraints = $constraints;
		$this->ifNotExists = $ifNotExists;
		$this->temporary = $temporary;
	}
	
	function execute()
	{
		$table_name = $this->fullTableName[2];
		$schema =& $this->environment->_find_schema($this->fullTableName[0], $this->fullTableName[1]);
		if($schema === false) {
			return false;
		} else if($schema->getTable($table_name) !== false) {
			if(empty($this->ifNotExists)) {
				return $this->environment->_set_error("A relation named {$table_name} already exists");
			} else {
				return true;
			}
		}
		
		$table =& $schema->createTable($table_name, $this->columns, $this->temporary);
		if($table !== false)
		{
			$tableDef =& $table->getDefinition();
			$columnIndicies = array_flip($tableDef->getColumnNames());
			foreach($this->constraints as $constraint_name => $constraint_data)
			{
				$constraint_columns = array();
				foreach($constraint_data['columns'] as $column)
					$constraint_columns[] = $columnIndicies[$column];
				$schema->createKey($constraint_name, $constraint_data['type'], $constraint_columns, $table);
			}
			$master =& $this->environment->_get_master_schema();
			$master->addTable($table);
			return true;
		} else {
			return false;
		}
	}
}

?>