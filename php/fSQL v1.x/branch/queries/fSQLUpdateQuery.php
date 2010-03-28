<?php

class fSQLUpdateQuery extends fSQLDMLQuery
{
	var $tableNamePieces;
	
	var $setArray;
	
	var $where;
	
	var $ignore;
	
	
	function fSQLUpdateQuery(&$environment, $tableNamePieces, $setArray, $where, $ignore)
	{
		parent::fSQLDMLQuery($environment);
		$this->tableNamePieces = $tableNamePieces;
		$this->setArray = $setArray;
		$this->where = $where;
		$this->ignore = $ignore;
	}
	
	////Update data in the DB
	function execute()
	{
		$this->affected = 0;
		
		$table =& $this->environment->_find_table($this->tableNamePieces);
		if($table === false) {
			return false;
		} else if($table->isReadLocked()) {
			return $this->environment->_error_table_read_lock($this->tableNamePieces);
		}
		
		$columns = $table->getColumns();
		$columnNames = array_keys($columns);
		$cursor =& $table->getWriteCursor();
			
		$col_indicies = array_flip($columnNames);
		$updates = array();
		
		$code = "";
		foreach($this->setArray as $set) {
			list($column, $value) = $set;	
			$columnDef = $columns[$column];
			$new_value = $this->environment->_parse_value($columnDef, $value);
			if($new_value === false)
				return $this->environment->_set_error('Unknown value: '.$value);
			if(is_string($new_value))
				$new_value = $this->environment->_prep_for_insert($new_value);
			$col_index = $col_indicies[$column];
			$updates[$col_index] = $new_value;
			$code .= "\t\t\$cursor->updateField($col_index, $new_value);\r\n";
		}
		
		$code .= "\t\t\$this->affected++;\r\n";
		
		if($this->where)
			$code = "\tif({$this->where}) {\r\n$code\r\n\t}";

			eval(<<<EOC
for( \$cursor->first(); !\$cursor->isDone(); \$cursor->next())
{
	\$entry = \$cursor->getRow();
$code
}
EOC
			);

			if($this->affected)
			{
				if($this->environment->auto)
					$table->commit();
				else if(!in_array($table, $this->environment->updatedTables))
					$this->environment->updatedTables[] =& $table;
			}
			
			return true;
	}	
}

?>