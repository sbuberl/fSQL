<?php

class fSQLView extends fSQLTable
{
	var $query = null;
	var $columns = null;
	var $entries = null;
	
	function fSQLView($name, &$schema)
	{
		parent::fSQLTable($name, $schema);
	}
	
	function close()
	{
		parent::close();
		unset($this->query);
		unset($this->columns);
	}
	
	function define($query, $columns)
	{
		return false;
	}
	
	function setQuery($query)
	{
		$this->query = $query;
	}
	
	function getQuery()
	{
		return $this->query;
	}
	
	function execute()
	{
		$env =& $this->schema->getDatabase()->getEnvironment();
		$rs_id = $env->query($this->query);
		$rs =& $env->get_result_set($rs_id);
		if($rs !== false)
		{
			if($this->getColumns() === null)
				$this->definition->setColumns($rs->columns);
			$this->entries = $rs->data;
			$env->free_result($rs_id);
			return true;
		}
		else
			return false;
	}
}

class fSQLTemporaryView extends fSQLView
{
	var $rcursor = null;
	
	function fSQLTemporaryView($name, &$schema)
	{
		parent::fSQLView($name, $schema);
		$this->definition =& new fSQLMemoryTableDef();
	}

	function define($query, $columns)
	{
		$this->setQuery($query);
		$this->definition->setColumns($columns);
		return $this->execute();
	}
	
	function drop()
	{
		$this->close();
	}
	
	function temporary()
	{
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
	
	function getEntries() {
		return $this->entries;
	}
	
	function &getCursor()
	{
		if($this->rcursor === null)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}
}

class fSQLStandardView extends fSQLView
{
	var $rcursor = null;
	var $queryLockFile = null;
	var $queryFile = null;
	var $queryLoad = null;
	var $lock = null;
	
	function fSQLStandardView($name, &$schema)
	{
		parent::fSQLView($name, $schema);
		$path_to_schema = $schema->getPath();
		$def_path = $path_to_schema.$name.'.view';
		$columns_path = $path_to_schema.$name.'.columns';
		$this->definition =& new fSQLStandardTableDef($columns_path);
		$this->queryLockFile =& new fSQLFile($def_path.'.lock.cgi');
		$this->queryFile =& new fSQLFile($def_path.'.cgi');
	}
	
	function define($query, $columns)
	{
		list($msec, $sec) = explode(' ', microtime());
		$this->queryLoad = $sec.$msec;

		// create the view lock
		$this->queryLockFile->acquireWrite();
		$queryLock = $this->queryLockFile->getHandle();
		ftruncate($queryLock, 0);
		fwrite($queryLock, $this->queryLoad);
		
		// create the view file
		$this->queryFile->acquireWrite();
		$definition = $this->queryFile->getHandle();
		ftruncate($definition, 0);
		fwrite($definition, $query);
		
		$this->queryFile->releaseWrite();	
		$this->queryLockFile->releaseWrite();
		
		$this->setQuery($query);
		$this->definition->setColumns($columns);
		$this->execute();
	}
	
	function drop()
	{
		if($this->lock === null)
		{
			$this->definition->drop();
			unlink($this->queryFile->getPath());
			unlink($this->queryLockFile->getPath());
			$this->close();
			return true;
		}
		else
			return false;
	}
	
	function temporary()
	{
		return false;
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
	
	function getEntries() {
		return $this->entries;
	}
	
	function &getCursor()
	{
		if($this->rcursor === NULL)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}
	
	function _loadView()
	{
		$this->queryLockFile->acquireRead();
		$lock = $this->queryLockFile->getHandle();
		
		$modified = fread($lock, 20);
		if($this->queryLoad === null || $this->queryLoad < $modified)
		{
			$this->queryLoad = $modified;

			$this->queryFile->acquireRead();
			$dataHandle = $this->queryFile->getHandle();

			$this->query = fgets($dataHandle, 4096);

			$this->queryFile->releaseRead();
		}

		$this->queryLockFile->releaseRead();

		return true;
	}
	
	function execute()
	{
		$this->definition->getColumns();
		$this->_loadView();
		return parent::execute();
	}
	
	function isReadLocked()
	{
		return $this->lock === 'r';
	}

	function readLock()
	{
		$success = $this->definition->readLock() && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
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