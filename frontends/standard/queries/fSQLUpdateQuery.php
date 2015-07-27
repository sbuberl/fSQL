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
		
		$tableDef =& $table->getDefinition();
		$columns = $tableDef->getColumns();
		$columnNames = array_keys($columns);
		$cursor =& $table->getWriteCursor();
			
		$col_indicies = array_flip($columnNames);
		
		// find all unique keys and columns to watch.
		$keys = $table->getKeys();
		$uniqueKeys = array();
		$unique_key_columns  = array();
		foreach(array_keys($keys) as $k)
		{
			$key =& $keys[$k];
			if($key->getType() & FSQL_KEY_UNIQUE)
			{
				$unique_key_columns = array_merge($unique_key_columns, $key->getColumns());
				$uniqueKeys[] =& $keys[$k];
			}
		}
		
		// generate code from SET info 
		$code = '';
		$updates = array();
		foreach($this->setArray as $set) {
			list($column, $value) = $set;
			$new_value = $this->environment->_parse_value($columns[$column], $value);
			if($new_value === false)
				return $this->environment->_set_error('Unknown value: '.$value);
			$new_value = $this->environment->_prep_for_insert($new_value);
			$col_index = $col_indicies[$column];
			$updates[$col_index] = "$col_index => $new_value";
		}
		
		$code .= "\t\t\$cursor->updateRow(array(".implode(',', $updates)."));\r\n";
		// find all updated columns that are part of a unique key
		// if there are any, call checkUnique to validate still unique.		
		$updated_key_columns = array_intersect(array_keys($updates), $unique_key_columns);
		if(!empty($updated_key_columns))
			$code .= "\t\tif(\$this->checkUnique(\$uniqueKeys, \$rowId, \$cursor->getRow()) === false) return false;\r\n";
		
		$code .= "\t\t\$this->affected++;\r\n";
		
		if($this->where)
			$code = "\tif({$this->where}) {\r\n$code\r\n\t}";
		
		$success = eval(<<<EOC
for( \$rowId = \$cursor->first(); !\$cursor->isDone(); \$rowId = \$cursor->next())
{
	\$entry = \$cursor->getRow();
$code
}
return true;
EOC
		);

		if($success)
			return $this->commit($table);
		else
			return false;
	}
	
	function checkUnique($uniqueKeys, $row_id, $updated_row)
	{
		foreach(array_keys($uniqueKeys) as $k)
		{
			$key =& $uniqueKeys[$k];
			$new_value = $key->extractIndex($updated_row);
			$indexed_row_id = $key->lookup($new_value);
			if($indexed_row_id !== false && $indexed_row_id !== $row_id)
				return $this->environment->_set_error('Duplicate unique violation during UPDATE');
		}
		return true;
	}
}

?>