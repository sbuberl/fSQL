<?php

if(!class_exists('fSQLStandardDriver'))
	require FSQL_INCLUDE_PATH.'/drivers/fSQLStandardDriver.php';

class fSQLDefaultDriver extends fSQLStandardDriver
{
	function isAbstract()
	{
		return false;
	}
	
	function &newSchemaObj(&$db, $name)
	{
		$schema =& new fSQLDefaultSchema($db, $name);
		return $schema;
	}
	
	function &newTableObj(&$schema, $name)
	{
		$table =& new fSQLDefaultTable($schema, $name);
		return $table;
	}
	
	function &newTableDefObj(&$schema, $table_name)
	{
		$def =& new fSQLDefaultTableDef($schema, $table_name);
		return $def;
	}
}

class fSQLDefaultSchema extends fSQLStandardSchema
{	
	function &createKey($name, $type, $columns, &$table)
	{
		$key = false;
		$schema =& $table->getSchema();
		if($type === FSQL_KEY_PRIMARY)
		{
			$engine = 'HASH_FILE';
			$file = $table->getName().'.primary.cgi';
			$key =& new fSQLDefaultKey($schema->getPath().$file);
		}
		else
		{
			$engine = 'MEM';
			$file = null;
			$key =& new fSQLMemoryKey($type);
		}
		
		$key->create($columns);
		$tableDef =& $table->getDefinition();
		$tableDef->addKey($name, $type, $columns, $engine, $file);
		return $key;
	}
}

class fSQLDefaultTableDef extends fSQLStandardTableDef
{
	var $primaryKeyName = null;
	
	function addKey($name, $type, $columns, $engine, $fileName)
	{
		if($type === FSQL_KEY_PRIMARY)
			$this->primaryKeyName = $name;
		$this->keys[$name] = array('type' => $type, 'columns' => $columns, 'engine'=>$engine, 'file' => $fileName);
		$this->setColumns($this->columns);
	}
	
	function _buildReadWriteFuncs()
	{
		if(empty($this->columns))
			return false;
		
		$readString = '^';
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
if(preg_match("/{$readString}/", \$row, \$entry))
{
	array_shift(\$entry);
	return array($fullTranslateCode); 
}
else
	return false;
EOC;
		
		$this->readFunction = create_function('$row', $readCode);
	}
	
	function getPrimaryKeyName()
	{
		$this->getColumns();
		return $this->primaryKeyName;
	}
	
	function getColumns()
	{
		$success = true;
		$this->columnsLockFile->acquireRead();
		if($this->columnsLockFile->wasModified())
		{
			$this->columnsLockFile->accept();
			
			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();
			
			while(!feof($columnsHandle))
			{
				$line =	file_read_line($columnsHandle);
				if(empty($line))
					break;
				
				if(preg_match("/(\S+): (column|key);(.*)/", $line, $matches))
				{
					list(, $name, $obType, $the_rest)  = $matches;
					if($obType === 'column' && preg_match("/([a-z][a-z]?);(.*);(0|1);(-?\d+(?:\.\d+)?|'(.*)'|NULL);(0|1);/", $the_rest, $local_matches))
					{
						$type = $local_matches[1];
						$default = $local_matches[4];
						if($default === 'NULL')
							$default = null;
						else if($default{0} === '\'')
							$default = $local_matches[5];
						else if($type === FSQL_TYPE_INTEGER)
							$default = (int) $default;
						else if($type === FSQL_TYPE_FLOAT)
							$default = (float) $default;
						
						$restraint = '';
						if(preg_match_all("/'.*?(?<!\\\\)'/", $local_matches[2], $restraint))
							$restraint = $restraint[0];
						
						$this->columns[$name] = array(
							'type' => $type, 'auto' => (bool) $local_matches[3], 'default' => $default, 'key' => 'n', 'null' => (bool) $local_matches[6], 'restraint' => $restraint
						);
						
						continue;
					}
					else if($obType === 'key' && preg_match("/(\d+);(\d+(?:,\d+)*);(MEM|HASH_FILE);([\w.]*)/", $the_rest, $local_matches))
					{
						list(, $type, $columns, $engine, $file) = $local_matches;
						$columns = explode(',', $columns);
						
						if($type == FSQL_KEY_PRIMARY)
							$this->primaryKeyName = $name;
						
						$this->keys[$name] = array(
							'type' => (int) $type, 'columns' => $columns, 'engine' => $engine, 'file' => (!empty($file) ? $file : null)
						);
						
						continue;
					}
				}
				
				// should only make it here on an error
				$success = false;
				break;
			}

			$this->columnsFile->releaseRead();
			
			if($success)
				$this->_buildReadWriteFuncs();
		}
		
		$this->columnsLockFile->releaseRead();
		
		return $success ? $this->columns : false;
	}
	
	function setColumns($columns)
	{
		$this->columns = $columns;
		$this->_buildReadWriteFuncs();
		
		$toprint = '';
		if(!empty($columns))
		{
			foreach($columns as $name => $column)
			{
				$default = $column['default'];
				if($default === null)
					$default = 'NULL';
				else if(is_string($default))
					$default = "'".$default."'";
				
				$restraint = is_array($column['restraint']) ? implode(',', $column['restraint']) : '';
				
				$toprint .= sprintf("%s: column;%s;%s;%d;%s;%d;\r\n", $name, $column['type'], $restraint, $column['auto'], $default, $column['null']);
			}
		}
		
		foreach($this->keys as $name => $key)
		{
			$key_column_list = implode(',', $key['columns']);
			$toprint .= sprintf("%s: key;%d;%s;%s;%s\r\n", $name, $key['type'], $key_column_list, $key['engine'], $key['file']);
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
 * Class for the Default fSQL tables that are saved to the filesystem.
 */
class fSQLDefaultTable extends fSQLStandardTable
{
	var $primary = null;
	
	function create($columnDefs)
	{
		$this->definition->setColumns($columnDefs);

		// create the data lock
		$this->dataLockFile->write();
		$this->dataLockFile->reset();
		
		// create the data file
		$this->dataFile->acquireWrite();
		fwrite($this->dataFile->getHandle(), '');
		$this->dataFile->releaseWrite();

		return $this;
	}
	
	function close()
	{
		unset($this->primary);
		parent::close();
	}
	
	function &getWriteCursor()
	{
		$this->_loadEntries();

		if($this->wcursor === null)
			$this->wcursor = new fSQLDefaultWriteCursor($this->entries, $this);
		
		return $this->wcursor;
	}

	function _checkPrimaryKeyExists()
	{
		if(!isset($this->primary))
		{
			$this->primary =& $this->getKey($this->definition->getPrimaryKeyName());
			$this->primary->load();
		}
	}
	function _loadEntries()
	{
		$this->dataLockFile->acquireRead();
		if($this->dataLockFile->wasModified())
		{
			$this->dataLockFile->accept();

			$this->_checkPrimaryKeyExists();
			
			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();
			$entries = array();
			
			$readFunction = $this->definition->getReadFunction();
			while(!feof($dataHandle)) {
				$line = rtrim(file_read_line($dataHandle));
				if(!empty($line))
				{
					$entry = $readFunction($line);
					$row_id = $this->primary->key[$entry[0]];
					$entries[$row_id] = $entry;
				}
			}
			
			$this->entries = $entries;

			$this->dataFile->releaseRead();
		}

		$this->dataLockFile->releaseRead();

		return true;
	}
	
	function _printRowToString($entry)
	{
		$toprint = '';
		foreach($entry as $value) {
			if($value === null)
				$toprint .= 'NULL;';
			else if(is_string($value))
				$toprint .= "'$value';";
			else
				$toprint .= $value.';';
		}
		return $toprint."\r\n";
	}
	
	function commit()
	{
		$this->_checkPrimaryKeyExists();
		
		$this->dataLockFile->acquireWrite();
		
		$this->dataFile->acquireWrite();
		$dataHandle = $this->dataFile->getHandle();
		
		$writeCursor = $this->wcursor;
		
		// handle updates and inserts first
		if(!empty($writeCursor->updatedRows) || !empty($writeCursor->deletedRows))
		{
			$delta = 0;
			$readFunction = $this->definition->getReadFunction();
			$updatedRows = $writeCursor->updatedRows;
			$changedRows = array_merge(array_keys($updatedRows), $writeCursor->deletedRows);
			
			$start_id = min($changedRows);
			$start = $this->primary->positions[$start_id];
			fseek($dataHandle, 0, SEEK_END);
			$length = ftell($dataHandle) - $start;
			fseek($dataHandle, $start, SEEK_SET);
			$contents = fread($dataHandle, $length);
			
			$rows = $this->primary->getSortedSubset($start);
			foreach($rows as $row_id)
			{
				$offset = $this->primary->positions[$row_id];
				if(isset($updatedRows[$row_id]))
				{
					$changes = $updatedRows[$row_id];
					$str_offset = $offset - $start + $delta;
					$length = $this->primary->lengths[$row_id];
					$row = substr($contents, $str_offset, $length);
					$entry = $readFunction($row);
					foreach($changes as $col_index => $new_value)
					{
						$entry[$col_index] = $new_value;
					}
					$newRow = $this->_printRowToString($entry);
					$newLength = strlen($newRow);
					if($delta !== 0)
						$this->primary->updatePosition($row_id, $offset + $delta);
					if($length !== $newLength)
						$this->primary->updateLength($row_id, $newLength);
					$delta += $newLength - $length;
					$contents = substr_replace($contents, $newRow, $str_offset, $length);
				}
				else if(in_array($row_id, $writeCursor->deletedRows))
				{
					$length = $this->primary->lengths[$row_id];
					$str_offset = $offset - $start + $delta;
					$contents = substr_replace($contents, '', $str_offset, $length);
					$this->primary->deleteFileInfo($row_id);
					$delta -= $length;
				}
				else if($delta !== 0)
				{
					$this->primary->updatePosition($row_id, $offset + $delta);
				}
			}
			
			ftruncate($dataHandle, $start);
			fseek($dataHandle, 0, SEEK_END);
			fwrite($dataHandle, $contents, strlen($contents));
		}
		
		// append new rows
		fseek($dataHandle, 0, SEEK_END);
		$newRows = $writeCursor->getNewRows();
		foreach ( $newRows as $newRowId => $newRow ) {
			$line = $this->_printRowToString($newRow);
			$length = strlen($line);
      		$this->primary->setFileInfo($newRowId, ftell($dataHandle), $length);
      		fwrite($dataHandle, $line, $length);
		}
		
		$this->primary->save();
		
		$this->dataLockFile->write();
		$this->dataLockFile->releaseWrite();

		$this->wcursor->close();
		$this->wcursor = null;
		
		return true;
	}

	function drop()
	{
		if($this->lock === null)
		{
			$keys = $this->getKeys();
			if(!empty($keys))
			{
				foreach(array_keys($keys) as $k)
				{
					$key =& $keys[$k];
					$key->drop();
				}
			}
			$this->definition->drop();
			$this->dataFile->drop();
			$this->dataLockFile->drop();
			$this->close();
			return true;
		}
		else
			return false;
	}
}

class fSQLDefaultKey extends fSQLKey
{
	var $addOnly = true;
	
	var $addedRows = array();
	
	var $keyFile;
	
	var $key = array();
	
	var $lengths = array();
	
	var $positions = array();
	
	var $columns;
	
	function fSQLDefaultKey($full_path)
	{
		$this->keyFile = new fSQLFile($full_path);
	}
	
	function addEntry($row, $key)
	{
		$this->key[$key] = $row;
		$this->lengths[$row] = false;
		$this->addedRows[] = $row;
		return true;
	}
	
	function close()
	{
		if(isset($this->keyFile))
			$this->keyFile->close();
		unset($this->addedRows, $this->keyFile, $this->key, $this->lengths, $this->positions, $this->columns);
		return true;
	}
	
	function create($columns)
	{
		$this->columns = $columns;
		return $this->save(true);
	}
	
	function getType()
	{
		return FSQL_KEY_PRIMARY;
	}
	
	function deleteEntry($row)
	{
		$index = array_search($row, $this->key);
		unset($this->key[$index]);
		$this->addedOnly = false;
		return true;
	}
	
	function deleteFileInfo($row)
	{
		unset($this->positions[$row]);
		unset($this->lengths[$row]);
	}
	
	function drop()
	{
		$this->keyFile->drop();
		$this->close();
		return true;
	}
	
	function getColumns()
	{
		return $this->columns;
	}
	
	function getSortedSubset($start)
	{
		$flip = array_flip($this->positions);
		ksort($flip);
		$start_index = array_search($start, array_keys($flip));
		$subset = array_slice(array_values($flip), $start_index);
		return $subset;
	}
	
	function load()
	{
		$this->keyFile->acquireRead();
		$keyHandle = $this->keyFile->getHandle();
		
		$this->key = array();
		$this->positions = array();
		$this->addOnly = true;
		$this->addedRows = array();
		if(!feof($keyHandle))
		{
			$line = rtrim(file_read_line($keyHandle));
			if($line !== '' && preg_match('/^(\d+(?:,\d+)*|none)$/', $line, $matches))
			{
				if($line !== 'none')
					$this->columns = array_map('intval', explode(',', $line));
				else
					$this->columns = null;
			}
				
			while(!feof($keyHandle))
			{
				$line = rtrim(file_read_line($keyHandle));
				if($line !== '' && preg_match('/^([\da-f]+):(\d+):(\d+):(.*)$/i', $line, $matches))
				{
					list(, $row_id, $offset, $length, $value) = $matches;
					$this->key[$value] = $row_id;
					$this->lengths[$row_id] = (int) $length;
					$this->positions[$row_id] = (int) $offset;
				}
			}
		}
		
		$this->keyFile->releaseRead();
		return true;
	}
	
	function lookup($key)
	{
		if(!isset($this->columns))
		{
			$this->load();
			if(!isset($this->columns))
				return false;
		}
		
		if(is_array($key))
			$key = serialize($key);
		
		if(array_key_exists($key, $this->key))
			return $this->key[$key];
		else
			return false;
	}
	
	function save($creating = false)
	{
		$this->keyFile->acquireWrite();
		$keyHandle = $this->keyFile->getHandle();
		
		$reverseKey = array_flip($this->key);
		if($creating || !$this->addOnly)
		{
			if(!$creating)
				ftruncate($keyHandle, 0);
			$columnsLine = isset($this->columns) ? implode(',', $this->columns) : 'none';
			fwrite($keyHandle, "$columnsLine\r\n");
			foreach($reverseKey as $row => $value)
			{
				$pos = $this->positions[$row];
				$length = $this->lengths[$row];
				fwrite($keyHandle, "$row:$pos:$length:$value\r\n");
			}
		}
		else
		{
			fseek($keyHandle, 0, SEEK_END);
			foreach($this->addedRows as $row)
			{
				$value = $reverseKey[$row];
				$pos = $this->positions[$row];
				$length = $this->lengths[$row];
				fwrite($keyHandle, "$row:$pos:$length:$value\r\n");
			}
		}
		
		$this->keyFile->releaseWrite();
		$this->addOnly = true;
		$this->addedRows = array();
		return true;
	}
	
	function setFileInfo($row, $pos, $length)
	{
		$this->positions[$row] = $pos;
		$this->lengths[$row] = $length;
	}
	
	function updateEntry($row, $key)
	{
		$index = array_search($row, $this->key);
		unset($this->key[$index]);
		$this->key[$key] = $row;
	}
	
	function updatePosition($index, $pos)
	{
		$this->positions[$index] = $pos;
		$this->addOnly = false;
	}
	
	function updateLength($index, $length)
	{
		$this->lengths[$index] = $length;
		$this->addOnly = false;
	}
}

class fSQLDefaultWriteCursor extends fSQLStandardWriteCursor
{
	function appendRow($entry, $rowid = false)
	{
		$rowid = md5(uniqid(mt_rand(), true)); // ignore rowid param
		return parent::appendRow($entry, $rowid);
	}
}

?>