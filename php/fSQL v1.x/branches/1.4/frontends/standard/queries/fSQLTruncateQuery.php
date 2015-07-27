<?php

class fSQLTruncateQuery extends fSQLQuery
{
	function query($query)
	{
		if(preg_match('/\ATRUNCATE\s+TABLE\s+(.*)[;]?\Z/is', $query, $matches)) {
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $matches)) {
					$table_name_pieces = $this->environment->_parse_table_name($matches[1]);
					$table =& $this->environment->_find_table($table_name_pieces);
					if($table === false)
						return false;
					else if($table->isReadLocked())
						return $this->environment->_error_table_read_lock($table_name_pieces);
					
					$tableDef =& $table->getDefinition();
					$columns = $tableDef->getColumns();
					$table_name = $table->getName();
					$db->dropTable($table_name);
					$db->createTable($table_name, $columns);
				} else {
					return $this->environment->_set_error('Parse error in table listing');
				}
			}
		} else {
			return $this->environment->_set_error('Invalid TRUNCATE query');
		}
		
		return true;
	}
}

?>