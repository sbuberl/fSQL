<?php

class fSQLInsertQuery extends fSQLDMLQuery
{
	var $insert_id = null;
	
	var $fullTableName;
	
	var $columnNames;
	
	var $dataValues;
	
	var $replace;
	
	var $ignore;
	
	function fSQLInsertQuery(&$environment, $fullTableName, $columnNames, $dataValues, $replace, $ignore)
	{
		parent::fSQLDMLQuery($environment);
		$this->fullTableName = $fullTableName;
		$this->columnNames = $columnNames;
		$this->dataValues = $dataValues;
		$this->replace = $replace;
		$this->ignore = $ignore;
	}
	
	function execute()
	{
		$table =& $this->environment->_find_table($this->fullTableName);
		if($table === false) {
			return false;
		} else if($table->isReadLocked()) {
			return $this->environment->_error_table_read_lock($this->fullTableName);
		}

		$tableDef = $table->getDefinition();
		$tableColumns = $tableDef->getColumns();
		$tableCursor =& $table->getWriteCursor();

		$newentry = array();
		$col_index = -1;
		
		////Load Columns & Data for the Table
		foreach($tableColumns as $col_name => $columnDef)  {

			++$col_index;
			
			$data = trim($this->dataValues[$col_index]);				
			$data = strtr($data, array("$" => "\$", "\$" => "\\\$"));
			
			////Check for Auto_Increment
			if((!strcasecmp($data, 'NULL') || strlen($data) === 0 || !strcasecmp($data, 'AUTO')) && $columnDef['identity']) {
				$id = $table->nextValueFor($col_name);
				if($id !== false)
				{
					$this->insert_id = $id;
					$newentry[$col_index] = $this->insert_id;
				}
				else
					return $this->environment->_set_error('Error getting next value for identity column: '.$col_name);
			}
			else
			{
				$data = $this->environment->_parse_value($columnDef, $data);
				if($data === false)
					return false;
				$newentry[$col_index] = $data;
			}
		}
		
		$keys = $table->getKeys();
		if(!empty($keys))
		{
			foreach(array_keys($keys) as $k)
			{
				$key =& $keys[$k];
				if($key->getType() & FSQL_KEY_UNIQUE)
				{
					$indexValue = $key->extractIndex($newentry);
					if($indexValue !== false)
					{
						$rowid = $key->lookup($indexValue);
						if($rowid !== false)
						{
							if($this->replace)
							{
								// may have already ben deleted so check return
								if($tableCursor->find($rowid))
								{
									$tableCursor->deleteRow();
									$this->affected++;
								}
							}
							else if(!$this->ignore)
								return $this->environment->_set_error("Duplicate value found on key");
							else
								return true;
						}
					}
				}
			}
		}
		
		$tableCursor->appendRow($newentry);
		
		$this->affected++;
		
		return $this->commit($table);
	}
}

?>