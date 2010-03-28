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

		$tableColumns = $table->getColumns();
		$tableCursor =& $table->getWriteCursor();

		$unique_keys = array(0 => array('type' => 'p', 'columns' => array()));
		$newentry = array();
		$col_index = -1;
		
		////Load Columns & Data for the Table
		foreach($tableColumns as $col_name => $columnDef)  {

			++$col_index;
			
			$data = trim($this->dataValues[$col_index]);				
			$data = strtr($data, array("$" => "\$", "\$" => "\\\$"));
			
			////Check for Auto_Increment
			if((!strcasecmp($data, 'NULL') || strlen($data) === 0 || !strcasecmp($data, 'AUTO')) && $columnDef['auto']) {
				$tableCursor->last();
				$lastRow = $tableCursor->getRow();
				if($lastRow !== NULL)
					$this->insert_id = $lastRow[$col_index] + 1;
				else
					$this->insert_id = 1;
				$newentry[$col_index] = $this->insert_id;
			}
			else
			{
				$data = $this->environment->_parse_value($columnDef, $data);
				if($data === false)
					return false;
				$newentry[$col_index] = $data;
			}
			
			////See if it is a PRIMARY KEY or UNIQUE
			if($columnDef['key'] === 'p')
				$unique_keys[0]['columns'][] = $col_index;
			else if($columnDef['key'] === 'u')
				$unique_keys[] = array('type' => 'u', 'columns' => array($col_index));	
		}
		
		if(!empty($unique_keys[0]['columns']) || count($unique_keys) > 1) {
			$tableCursor->first();
			while(!$tableCursor->isDone()) {
				$row = $tableCursor->getRow();
				$do_delete = false;
				foreach($unique_keys as $unique_key) {
					$match_found = true;
					foreach($unique_key['columns'] as $col_index) {
						$match_found = $match_found && $row[$col_index] == $newentry[$col_index];
					}
					if($match_found) {
						if($this->replace)
							$do_delete = true;
						else if(!$this->ignore)
							return $this->environment->_set_error("Duplicate value found on key");
						else
							return true;
					}
				}
				
				if($do_delete) {
					$tableCursor->deleteRow();
					$this->affected++;
				}
				else
					$tableCursor->next();
			}
		}

		$tableCursor->appendRow($newentry);
		
		if($this->environment->auto)
			$table->commit();
		else if(!in_array($table, $this->environment->updatedTables))
			$this->environment->updatedTables[] =& $table;

		$this->affected++;
		
		return true;
	}
}

?>