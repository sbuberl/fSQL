<?php

/**
 * Base class for fSQL tables
 */
class fSQLTable
{
	var $name = null;
	var $definition = null;
	var $schema = null;

	function fSQLTable($name, &$schema)
	{
		$this->name = $name;
		$this->schema =& $schema;
	}

	function getName()
	{
		return $this->name;
	}
	
	function &getDefinition()
	{
		return $this->definition;
	}

	function &getSchema()
	{
		return $this->schema;
	}

	function rename($new_name)
	{
		$this->name = $new_name;
	}
	
	function close()
	{
		$this->name = null;
		$this->definition = null;
		$this->schema = null;
		return true;
	}
	
	function drop() { return false; }
	function temporary() { return false; }

	function getColumnNames() { return false; }
	function getColumns() {return false; }
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
	var $rcursor = null;
	var $wcursor = null;
	var $entries = null;

	function fSQLTemporaryTable($name, &$schema)
	{
		$this->name = $name;
		$this->definition =& new fSQLMemoryTableDef();
		$this->schema =& $schema;
	}

	function create($columnDefs)
	{
		$this->definition->setColumns($columnDefs);
		$this->entries = array();
	}

	function temporary() {
		return true;
	}
	
	function getColumnNames() {
		return array_keys($this->definition->getColumns());
	}
	
	function getColumns() {
		return $this->definition->getColumns();
	}
	
	function setColumns($columns) {
		$this->definition->setColumns($columns);
	}
	
	function &getCursor()
	{
		if($this->rcursor === null)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}

	function &getWriteCursor()
	{
		if($this->wcursor === null)
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
		$this->rcursor = null;
		$this->wcursor = null;
		$this->definition->drop();
		$this->definition = null;
		$this->entries = null;
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
	var $rcursor = null;
	var $wcursor = null;
	var $entries = null;
	var $data_load = null;
	var $dataLockFile = null;
	var $dataFile = null;
	var $lock = null;
	
	function fSQLStandardTable($name, &$schema)
	{
		$this->name = $name;
		$this->schema =& $schema;
		$path_to_schema = $schema->getPath();
		$columns_path = $path_to_schema.$name.'.columns';
		$data_path = $path_to_schema.$name.'.data';
		$this->definition = new fSQLStandardTableDef($columns_path);
		$this->dataLockFile = new fSQLFile($data_path.'.lock.cgi');
		$this->dataFile = new fSQLFile($data_path.'.cgi');
	}
	
	function create($columnDefs)
	{
		$this->definition->setColumns($columnDefs);
		
		list($msec, $sec) = explode(' ', microtime());
		$this->data_load = $sec.$msec;

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

	function temporary() {
		return false;
	}
	
	function getColumnNames() {
		return array_keys($this->definition->getColumns());
	}
	
	function getColumns() {
		return $this->definition->getColumns();
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

		if($this->wcursor === null)
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
		if($this->data_load === null || $this->data_load < $modified)
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
			$this->definition->drop();
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
		$success = $this->definition->unlock() && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
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
		$success = $this->definition->writeLock() && $this->dataLockFile->acquireWrite() && $this->dataFile->acquireWrite();
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
			$this->definition->unlock();
			$this->dataLockFile->releaseRead();
			$this->dataFile->releaseRead();
		}
		else if($this->lock === 'w')
		{
			$this->definition->unlock();
			$this->dataLockFile->releaseWrite();
			$this->dataFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

?>