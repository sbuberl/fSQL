<?php

/**
 * Base class for fSQL tables
 */
class fSQLTable
{
	var $name = NULL;
	var $database = NULL;

	function fSQLTable($name, &$database)
	{
		$this->name = $name;
		$this->database =& $database;
	}

	function getName()
	{
		return $this->name;
	}

	function &getDatabase()
	{
		return $this->database;
	}

	function rename($new_name)
	{
		$this->name = $new_name;
	}
	
	function drop() { return false; }
	function exists() { return false; }
	function temporary() { return false; }

	function getColumnNames() { return false; }
	function getColumns() { return false; }
	function setColumns($columns) { }
	
	function getCursor() { return null; }
	function getWriteCursor() { return null; }
	function getEntries() { return null; }
	
	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
}

/**
 * Class for temporary and in-memory tables.
 */
class fSQLTemporaryTable extends fSQLTable
{
	var $exists = false;
	var $rcursor = NULL;
	var $wcursor = NULL;
	var $columns = NULL;
	var $entries = NULL;

	function fSQLTemporaryTable($name, &$database)
	{
		$this->name = $name;
		$this->database =& $database;
	}

	function create($columnDefs)
	{
		$this->exists = true;
		$this->columns = $columnDefs;
		$this->entries = array();
	}
	
	function exists() {
		return $this->exists;
	}

	function temporary() {
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
	
	function &getCursor()
	{
		if($this->rcursor === NULL)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}

	function &getWriteCursor()
	{
		if($this->wcursor === NULL)
			$this->wcursor =& new fSQLWriteCursor($this->entries);
		
		return $this->wcursor;
	}
	
	function getEntries()
	{
		return $this->entries;
	}
	
	function commit()
	{

	}
	
	function rollback()
	{

	}

	// Free up all data
	function drop()
	{
		$this->rcursor = NULL;
		$this->wcursor = NULL;
		$this->columns = NULL;
		$this->entries = NULL;
	}

	/* Unnecessary for temporary tables */
	function isReadLocked() { return false; }
	function readLock() { return true; }
	function writeLock() { return true; }
	function unlock() { return true; }
}

/**
 * Class for the standard fSQL tables that are saved to the filesystem.
 */
class fSQLStandardTable extends fSQLTable
{
	var $rcursor = NULL;
	var $wcursor = NULL;
	var $columns = NULL;
	var $entries = NULL;
	var $columns_load = NULL;
	var $data_load = NULL;
	var $columnsLockFile = NULL;
	var $columnsFile = NULL;
	var $dataLockFile = NULL;
	var $dataFile = NULL;
	var $lock = NULL;
	
	function fSQLStandardTable($name, &$database)
	{
		$this->name = $name;
		$this->database =& $database;
		$path_to_db = $database->getPath();
		$columns_path = $path_to_db.$name.'.columns';
		$data_path = $path_to_db.$name.'.data';
		$this->columnsLockFile = new fSQLFile($columns_path.'.lock.cgi');
		$this->columnsFile = new fSQLFile($columns_path.'.cgi');
		$this->dataLockFile = new fSQLFile($data_path.'.lock.cgi');
		$this->dataFile = new fSQLFile($data_path.'.cgi');
	}
	
	function create($columnDefs)
	{
		$this->columns = $columnDefs;
		
		list($msec, $sec) = explode(' ', microtime());
		$this->columns_load = $this->data_load = $sec.$msec;

		// create the columns lock
		$this->columnsLockFile->acquireWrite();
		$columnsLock = $this->columnsLockFile->getHandle();
		ftruncate($columnsLock, 0);
		fwrite($columnsLock, $this->columns_load);
		
		// create the columns file
		$this->columnsFile->acquireWrite();
		$toprint = $this->_printColumns($columnDefs);
		fwrite($this->columnsFile->getHandle(), $toprint);
		
		$this->columnsFile->releaseWrite();	
		$this->columnsLockFile->releaseWrite();

		// create the data lock
		$this->dataLockFile->acquireWrite();
		$dataLock = $this->dataLockFile->getHandle();
		ftruncate($dataLock, 0);
		fwrite($dataLock, $this->data_load);
		
		// create the data file
		$this->dataFile->acquireWrite();
		fwrite($this->dataFile->getHandle(), "0\r\n");
		
		$this->dataFile->releaseWrite();	
		$this->dataLockFile->releaseWrite();
		
		return $this;
	}

	function _printColumns($columnDefs)
	{
		$toprint = count($columnDefs)."\r\n";
		foreach($columnDefs as $name => $column) {
			$default = $column['default'];
			if($default === NULL)
				$default = 'NULL';
			else if(is_string($default))
				$default = "'".$default."'";
			
			$restraint = is_array($column['restraint']) ? implode(',', $column['restraint']) : '';
			
			$toprint .= sprintf("%s: %s;%s;%d;%s;%s;%d;\r\n", $name, $column['type'], $restraint, $column['auto'], $default, $column['key'], $column['null']);
		}
		return $toprint;
	}
	
	function exists() {
		return file_exists($this->columnsFile->getPath());
	}

	function temporary() {
		return false;
	}
	
	function getColumnNames() {
		return array_keys($this->getColumns());
	}
	
	function getColumns() {
		$this->columnsLockFile->acquireRead();
		$lock = $this->columnsLockFile->getHandle();

		$modified = fread($lock, 20);
		if($this->columns_load === NULL || $this->columns_load < $modified)
		{
			$this->columns_load = $modified;
			
			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();

			$line = fgets($columnsHandle);			
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return NULL;
			}
			
			$num_columns = $matches[1];

			for($i = 0; $i < $num_columns; $i++) {
				$line =	fgets($columnsHandle);
				if(preg_match("/(\S+): ([a-z][a-z]?);(.*);(0|1);(-?\d+(?:\.\d+)?|'(.*)'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
					$type = $matches[2];
					$default = $matches[5];
					if($default === "NULL")
						$default = NULL;
					else if($default{0} === '\'')
						$default = $matches[6];
					else if($type === FSQL_TYPE_INTEGER)
						$default = (int) $default;
					else if($type === FSQL_TYPE_FLOAT)
						$default = (float) $default;
					$this->columns[$matches[1]] = array(
						'type' => $type, 'auto' => (bool) $matches[4], 'default' => $default, 'key' => $matches[7], 'null' => (bool) $matches[8]
					);
					preg_match_all("/'.*?(?<!\\\\)'/", $matches[3], $this->columns[$matches[1]]['restraint']);
					$this->columns[$matches[1]]['restraint'] = $this->columns[$matches[1]]['restraint'][0];
				} else {
					return NULL;
				}
			}

			$this->columnsFile->releaseRead();
		}
		
		$this->columnsLockFile->releaseRead();
		
		return $this->columns;
	}
	
	function &getCursor()
	{
		$this->_loadEntries();

		if($this->rcursor === NULL)
			$this->rcursor = new fSQLCursor($this->entries);
		
		return $this->rcursor;
	}
	
	function &getWriteCursor()
	{
		$this->_loadEntries();

		if($this->wcursor === NULL)
			$this->wcursor = new fSQLWriteCursor($this->entries);
		
		return $this->wcursor;
	}
	
	function getEntries()
	{
		$this->_loadEntries();
		return $this->entries;
	}

	function _loadEntries()
	{
		$this->dataLockFile->acquireRead();
		$lock = $this->dataLockFile->getHandle();
		
		$modified = fread($lock, 20);
		if($this->data_load === NULL || $this->data_load < $modified)
		{
			$this->data_load = $modified;

			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();

			$line = fgets($dataHandle);
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->dataFile->releaseRead();
				$this->dataLockFile->releaseRead();
				return NULL;
			}
	
			$num_entries = rtrim($matches[1]);
			$entries = array();
			
			if($num_entries != 0)
			{
				$columns = array_keys($this->getColumns());
				$skip = false;
	
				for($i = 0;  $i < $num_entries; $i++) {
					$line = rtrim(fgets($dataHandle, 4096));

					if(!$skip) {
						if(preg_match('/^(\d+):(.*)$/', $line, $matches))
						{
							$row = $matches[1];
							$data = trim($matches[2]);
						}
						else
							continue;
					}
					else {
						$data .= $line;
					}
				
					if(!preg_match("/(-?\d+(?:\.\d+)?|'.*?(?<!\\\\)'|NULL);$/", $line)) {
						$skip = true;
						continue;
					} else {
						$skip = false;
					}
				
					preg_match_all("#((-?\d+(\.\d+)?)|'(.*?(?<!\\\\))'|NULL);#s", $data, $matches);
					for($m = 0; $m < count($matches[0]); $m++) {
						$value = $matches[1][$m];
						if($value === 'NULL')  // null
							$entries[$row][$m] = NULL;
						else if($value{0} === '\'') //string
							$entries[$row][$m] = $matches[4][$m];
						else if(!empty($matches[3])) //float
							$entries[$row][$m] = (float) $value;
						else if(!empty($matches[2])) //int
							$entries[$row][$m] = (int) $value;
						else // other?
							$entries[$row][$m] = $value;
					}
				}
			}
			
			$this->entries = $entries;

			$this->dataFile->releaseRead();
		}

		$this->dataLockFile->releaseRead();

		return true;
	}

	function setColumns($columnDefs)
	{
		$this->columnsLockFile->acquireWrite();
		$lock = $this->columnsLockFile->getHandle();
		$modified = fread($lock, 20);
		
		$this->columns = $columnDefs;

		list($msec, $sec) = explode(' ', microtime());
		$this->columns_load = $sec.$msec;
		fseek($lock, 0, SEEK_SET);
		fwrite($lock, $this->columns_load);

		$this->columnsFile->acquireWrite();

		$toprint = $this->_printColumns($columnDefs);
		$columnsHandle = $this->columnsFile->getHandle();
		ftruncate($columnsHandle, 0);
		fwrite($columnsHandle, $toprint);
	
		$this->columnsFile->releaseWrite();
		$this->columnsLockFile->releaseWrite();
	}
	
	function commit()
	{
		if($this->getWriteCursor()->isUncommitted() === false)
			return;

		$this->dataLockFile->acquireWrite();
		$lock = $this->dataLockFile->getHandle();
		$modified = fread($lock, 20);
		
		if($this->data_load === NULL || $this->data_load >= $modified)
		{
			$toprint = count($this->entries)."\r\n";
			foreach($this->entries as $number => $entry) {
				$toprint .= $number.': ';
				foreach($entry as $key => $value) {
					if($value === NULL)
						$toprint .= 'NULL;';
					else if(is_string($value))
						$toprint .= "'$value';";
					else
						$toprint .= $value.';';
				}
				$toprint .= "\r\n";
			}
		} else {
			$toprint = "0\r\n";
		}
		
		list($msec, $sec) = explode(' ', microtime());
		$this->data_load = $sec.$msec;
		fseek($lock, 0, SEEK_SET);
		fwrite($lock, $this->data_load);
		
		$this->dataFile->acquireWrite();

		$dataHandle = $this->dataFile->getHandle();
		ftruncate($dataHandle, 0);
		fwrite($dataHandle, $toprint);
		
		$this->dataFile->releaseWrite();
		$this->dataLockFile->releaseWrite();

		$this->wcursor = null;
	}
	
	function rollback()
	{
		$this->data_load = 0;
		$this->uncommited = false;
	}

	function drop()
	{
		if($this->lock === null)
		{
			unlink($this->columnsFile->getPath());
			unlink($this->columnsLockFile->getPath());
			unlink($this->dataFile->getPath());
			unlink($this->dataLockFile->getPath());
			return true;
		}
		else
			return false;
	}

	function isReadLocked()
	{
		return $this->lock === 'r';
	}

	function readLock()
	{
		$success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
	 		&& $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
		if($success) {
			$this->lock = 'r';
			return true;
		} else {
			$this->unlock();  // release any locks that did work if at least one failed
			return false;
		}
	}

	function writeLock()
	{
		$success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
			&& $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
		if($success) {
			$this->lock = 'w';
			return true;
		} else {
			$this->unlock();  // release any locks that did work if at least one failed
			return false;
		}
	}

	function unlock()
	{
		if($this->lock === 'r')
		{
			$this->columnsLockFile->releaseRead();
			$this->columnsFile->releaseRead();
			$this->dataLockFile->releaseRead();
			$this->dataFile->releaseRead();
		}
		else if($this->lock === 'w')
		{
			$this->columnsLockFile->releaseWrite();
			$this->columnsFile->releaseWrite();
			$this->dataLockFile->releaseWrite();
			$this->dataFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

?>