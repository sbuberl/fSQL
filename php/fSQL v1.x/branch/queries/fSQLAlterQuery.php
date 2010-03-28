<?php

class fSQLAlterQuery extends fSQLQuery
{	
	function fSQLAlterQuery(&$environment)
	{
		parent::fSQLQuery($environment);
	}
	
	function query($query)
	{
		if(preg_match('/\AALTER\s+TABLE\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.*)/is', $query, $matches)) {
			list(, $table_name, $changes) = $matches;
			
			$table_name_pieces = $this->environment->_parse_table_name($table_name);
			$tableObj =& $this->environment->_find_table($table_name_pieces);
			if($tableObj === false)
				return false;
			else if($tableObj->isReadLocked())
				return $this->environment->_error_table_read_lock($table_name_pieces);
			
			$schema = $tableObj->getSchema();
			$columns =  $tableObj->getColumns();
			
			preg_match_all('/(?:ADD|ALTER|CHANGE|DROP|RENAME).*?(?:,|\Z)/is', trim($changes), $specs);
			for($i = 0; $i < count($specs[0]); $i++) {
				if(preg_match('/\AADD\s+(?:CONSTRAINT\s+`?[^\W\d]\w*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/is', $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];
					
					foreach($columns as $name => $column) {
						if($column['key'] === 'p') {
							return $this->environment->_set_error('Primary key already exists');
						}
					}
					
					$columnDef['key'] = 'p';
					$tableObj->setColumns($columns);
					
					return true;
				} else if(preg_match("/\ACHANGE(?:\s+(?:COLUMN))?\s+`?([^\W\d]\w*)`?\s+(?:SET\s+DEFAULT ((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\3))|DROP\s+DEFAULT)(?:,|;|\Z)/is", $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];
					if(isset($matches[2]))
						$default = $matches[2];
					else
						$default = 'NULL';
					
					if(!$columnDef['null'] && strcasecmp($default, 'NULL')) {
						if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
							if($columnDef['type'] === FSQL_TYPE_INTEGER)
								$default = (int) $matches[2];
							else if($columnDef['type'] === FSQL_TYPE_FLOAT)
								$default = (float) $matches[2];
							else if($columnDef['type'] === FSQL_TYPE_ENUM) {
								if(in_array($default, $columnDef['restraint']))
									$default = array_search($default, $columnDef['restraint']) + 1;
								else
									$default = 0;
							}
						} else {
							if($columnDef['type'] === FSQL_TYPE_INTEGER)
								$default = (int) $default;
							else if($columnDef['type'] === FSQL_TYPE_FLOAT)
								$default = (float) $default;
							else if($columnDef['type'] === FSQL_TYPE_ENUM) {
								$default = (int) $default;
								if($default < 0 || $default > count($columnDef['restraint'])) {
									return $this->environment->_set_error('Numeric ENUM value out of bounds');
								}
							}
						}
					} else if(!$columnDef['null']) {
						if($columnDef['type'] === FSQL_TYPE_STRING)
							// The default for string types is the empty string 
							$default = "''";
						else
							// The default for dates, times, and number types is 0
							$default = 0;
					}
					
					$columnDef['default'] = $default;
					$tableObj->setColumns($columns);
					
					return true;
				} else if(preg_match('/\ADROP\s+PRIMARY\s+KEY/is', $specs[0][$i], $matches)) {
					$found = false;
					foreach($columns as $name => $column) {
						if($column['key'] === 'p') {
							$columns[$name]['key'] = 'n';
							$found = true;
						}
					}
					
					if($found) {
						$tableObj->setColumns($columns);
						return true;
					} else {
						return $this->environment->_set_error('No primary key found');
					}
				}
				else if(preg_match('/\ARENAME\s+(?:TO\s+)?(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $specs[0][$i], $matches)) {
					return $this->environment->_do_rename($tableObj, $matches[1]);
				}
				else {
					return $this->environment->_set_error('Invalid ALTER query');
				}
			}
		} else {
			return $this->environment->_set_error('Invalid ALTER query');
		}
	}
}

?>