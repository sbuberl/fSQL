<?php

if(!class_exists('fSQLStandardDriver'))
	require FSQL_INCLUDE_PATH.'/drivers/fSQLStandardDriver.php';

class fSQLLegacyDriver extends fSQLStandardDriver
{
	function isAbstract()
	{
		return false;
	}
	
	function &newSchemaObj(&$db, $name)
	{
		$schema =& new fSQLLegacySchema($db, $name);
		return $schema;
	}
	
	function &newTableObj(&$schema, $name)
	{
		$table =& new fSQLLegacyTable($schema, $name);
		return $table;
	}
	
	function &newTableDefObj(&$schema, $table_name)
	{
		$def =& new fSQLLegacyTableDef($schema, $table_name);
		return $def;
	}
}

class fSQLLegacySchema extends fSQLStandardSchema
{
	function &createKey($name, $type, $columns, &$table)
	{
		$key = false;
		if($type === FSQL_KEY_PRIMARY)
		{
			$key =& new fSQLMemoryKey($type);
			$key->create($columns);
			$tableDef =& $table->getDefinition();
			$tableDef->addKey($name, $type, $columns, 'MEM', '');
		}
		return $key;
	}
}

class fSQLLegacyTableDef extends fSQLStandardTableDef
{
	function addKey($name, $type, $columns, $engine, $fileName)
	{
		$colLookup = array_keys($this->columns);
		$this->keys[$name] = array('type' => $type, 'columns' => $columns, 'engine'=>$engine, 'file' => $fileName);
		foreach($columns as $colIndex)
		{
			$colName = $colLookup[$colIndex];
			$oldKeyValue = $this->columns[$colName]['key'];
			if($type === FSQL_KEY_PRIMARY)
				$this->columns[$colName]['key'] = 'p'; 
			else if($type & FSQL_KEY_UNIQUE && $oldKeyValue !== 'p')
				$this->columns[$colName]['key'] = 'u';
			else if($oldKeyValue !== 'p' && $oldKeyValue !== 'u')
				$this->columns[$colName]['key'] = 'k'; 
		}
		$this->setColumns($this->columns);
		return true;
	}
	
	function _buildReadWriteFuncs()
	{
		if(empty($this->columns))
			return false;
		
		$readString = '^(\d+):\s*';
		$translate = '';
		$i = 0;
		foreach($this->columns as $col_name => $column)
		{
			$typeMatch = '';
			$translateCode = '';
			switch($column['type'])
			{
				case FSQL_TYPE_INTEGER:
				case FSQL_TYPE_ENUM:
				case FSQL_TYPE_TIMESTAMP:
				case FSQL_TYPE_YEAR:
					$typeMatch = '-?\d+';
					$translateCode =  "((int) \$entry[$i])";
					break;
				case FSQL_TYPE_FLOAT:
					$typeMatch = '-?\d+\.\d+';
					$translateCode =  "((float) \$entry[$i])";
					break;
				default:
					$typeMatch = "'.*?(?<!\\\\\\\\)'";
					$translateCode = "substr(\$entry[$i], 1, -1)";
					break;
			}
			if($column['null']) {
				$readString .= "($typeMatch|NULL);";
				$translate[] = "((\$entry[$i] !== 'NULL') ? $translateCode : null)";
			} else {
				$readString .= "($typeMatch);";
				$translate[] = $translateCode;
			}
			$i++;
		}
					
		$fullTranslateCode = implode(',', $translate);
		$readCode = <<<EOC
\$line = rtrim(file_read_line(\$dataHandle));
if(preg_match("/{$readString}/", \$line, \$entry))
{
	array_shift(\$entry);
	\$row = (int) array_shift(\$entry);
	\$entries[\$row] = array($fullTranslateCode); 
}
EOC;
		
		$this->readFunction = create_function('$dataHandle,&$entries', $readCode);
	}
	
	function getColumns()
	{
		$this->columnsLockFile->acquireRead();
		if($this->columnsLockFile->wasModified())
		{
			$this->columnsLockFile->accept();
			
			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();

			$line = file_read_line($columnsHandle);		
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return null;
			}
			
			// quite sad this is the only way to retrieve the table name at this point
			$table_name = str_replace(basename($this->columnsFile->getPath()), '.columns.cgi', '');
			
			$num_columns = (int) $matches[1];
			$this->keys = array();
			for($i = 0; $i < $num_columns; $i++) {
				$line =	file_read_line($columnsHandle);
				if(preg_match("/(\S+): ([a-z][a-z]?);(.*);(0|1);(-?\d+(?:\.\d+)?|'(.*)'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
					$type = $matches[2];
					$default = $matches[5];
					if($default === 'NULL')
						$default = null;
					else if($default{0} === '\'')
						$default = $matches[6];
					else if($type === FSQL_TYPE_INTEGER)
						$default = (int) $default;
					else if($type === FSQL_TYPE_FLOAT)
						$default = (float) $default;

					$restraint = '';
					if(preg_match_all("/'.*?(?<!\\\\)'/", $matches[3], $restraint))
						$restraint = $restraint[0];
					
					$key = $matches[7];
					if($key !== 'n')
					{
						if($key === 'p')
						{
							$key_name = $table_name.'_pk';
							$key_type = FSQL_KEY_PRIMARY;
						}
						if(!isset($this->keys[$key_name]))
							$this->keys[$key_name] = array('type' => $key_type, 'columns' => array($i), 'engine' => 'MEM', 'file' => '');
						else   // add a column
							$this->keys[$key_name]['columns'][] = $i;

					}
					$this->columns[$matches[1]] = array(
						'type' => $type, 'auto' => (bool) $matches[4], 'default' => $default, 'key' => $key, 'null' => (bool) $matches[8], 'restraint' => $restraint
					);
				} else {
					return null;
				}
			}

			$this->columnsFile->releaseRead();
			
			$this->_buildReadWriteFuncs();
		}
		
		$this->columnsLockFile->releaseRead();
		
		return $this->columns;
	}
	
	function setColumns($columns)
	{
		$this->columns = $columns;
		$this->_buildReadWriteFuncs();
		
		$toprint = count($columns)."\r\n";
		if(!empty($columns))
		{
			foreach($columns as $name => $column)
			{
				$default = $column['default'];
				if($default === null)
					$default = 'NULL';
				else if(is_string($default))
					$default = "'".$default."'";
				
				if(empty($column['key']))
				{
					$key = 'n';
					$this->columns[$name]['key'] = 'n';
				}
				else
					$key = $column['key'];
				
				$restraint = is_array($column['restraint']) ? implode(',', $column['restraint']) : '';
				
				$toprint .= sprintf("%s: %s;%s;%d;%s;%s;%d;\r\n", $name, $column['type'], $restraint, $column['auto'], $default, $key, $column['null']);
			}
		}
		
		$this->columnsLockFile->acquireWrite();
		$this->columnsLockFile->write();

		$this->columnsFile->acquireWrite();
		$columnsHandle = $this->columnsFile->getHandle();
		ftruncate($columnsHandle, 0);
		fwrite($columnsHandle, $toprint);
	
		$this->columnsFile->releaseWrite();
		$this->columnsLockFile->releaseWrite();
	}
}

/**
 * Class for the Legacy fSQL tables that are saved to the filesystem.
 */
class fSQLLegacyTable extends fSQLStandardTable
{	
	var $keys = array();
	
	function create($columnDefs)
	{
		$this->definition->setColumns($columnDefs);

		// create the data lock
		$this->dataLockFile->write();
		
		// create the data file
		$this->dataFile->acquireWrite();
		fwrite($this->dataFile->getHandle(), "0\r\n");
		$this->dataFile->releaseWrite();

		return $this;
	}

	function _loadEntries()
	{
		$this->dataLockFile->acquireRead();
		if($this->dataLockFile->wasModified())
		{
			$this->dataLockFile->accept();

			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();

			$line = file_read_line($dataHandle);
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->dataFile->releaseRead();
				$this->dataLockFile->releaseRead();
				return null;
			}
	
			$num_entries = (int) rtrim($matches[1]);
			$entries = array();
			
			if($num_entries != 0)
			{
				$readFunction = $this->definition->getReadFunction();
				for($i = 0;  $i < $num_entries; $i++) {
					$readFunction($dataHandle, $entries);
				}
			}
			
			$this->entries = $entries;

			$this->dataFile->releaseRead();
		}

		$this->dataLockFile->releaseRead();

		return true;
	}
	
	function _overwriteFile($dataHandle, $entries)
	{
		// build string to write
		$toprint = count($entries)."\r\n";
		foreach($entries as $number => $entry) {
			$toprint .= $this->_printRowToString($number, $entry);
		}
	
		ftruncate($dataHandle, 0);
		fwrite($dataHandle, $toprint);
	}
	
	function _printRowToString($number, $entry)
	{
		$toprint = $number.': ';
		foreach($entry as $key => $value) {
			if($value === null)
				$toprint .= 'NULL;';
			else if(is_string($value))
				$toprint .= "'$value';";
			else
				$toprint .= $value.';';
		}
		return "$toprint\r\n";
	}
	
	function commit()
	{
		$this->dataLockFile->acquireWrite();
		
		$writeCursor = $this->wcursor;
		$newRows = $writeCursor->getNewRows();
		
		// first check for data updates, and if so reload from file
		// then reapply updates
		if($this->dataLockFile->wasModified())
		{
			$this->_loadEntries();
			
			foreach($writeCursor->updatedRows as $rowid => $changes)
			{
				foreach($changes as $column => $value)
					$this->entries[$rowid][$column] = $value;
			}
			
			foreach($writeCursor->deletedRows as $rowid)
			{
				unset($this->entries[$rowid]);
			}
			
			$newRowsIds = array();
			foreach($newRows as $oldId => $row)
			{
				$this->entries[] = $row;
				$aKeys = array_keys($this->entries);
				$newRowsIds[] = end($aKeys);
			}
		}
		else
			$newRowsIds = array_keys($newRows);
		
		$this->dataFile->acquireWrite();
		$dataHandle = $this->dataFile->getHandle();
		
		// on updates and/or deletes, just rewrite the whole file.
		if(!empty($writeCursor->updatedRows) || !empty($writeCursor->deletedRows))
		{
			$this->_overwriteFile($dataHandle, $this->entries);
		}
		// on inserts only, append to file if we can
		else
		{
			$newFirst = count($this->entries)."\r\n";
			
			rewind($dataHandle);
			$oldFirst = file_read_line($dataHandle);
			rewind($dataHandle);
			
			// if the length of the first line did not change,
			// we can overwrite the first lineand successfully append.
			// othwerwise, need to overwrite the whole file.
			if(strlen($newFirst) === strlen($oldFirst))
			{
				fseek($dataHandle, 0, SEEK_END);
				foreach($newRowsIds as $rowId)
				{
					$line = $this->_printRowToString($rowId, $this->entries[$rowId]);
					fwrite($dataHandle, $line, strlen($line));
				}
			}
			else
				$this->_overwriteFile($dataHandle, $this->entries);
		}
		
		$this->dataLockFile->write();
		
		$this->dataFile->releaseWrite();
		$this->dataLockFile->releaseWrite();

		$this->wcursor->close();
		$this->wcursor = null;
		
		return true;
	}
}

?>