<?php
/**
 * Standard driver definitions file
 * 
 * This file is describes classes for standard driver module. 
 * @author Kaja Fumei <kaja.fumei@gmail.com>
 * @version 
 */

/**
 * 
 */
class fSQLStandardDriver extends fSQLDriver
{
	function &defineDatabase(&$environment, $name, $args)
	{
		$path = $args[0];
		$db =& new fSQLStandardDatabase($this, $environment, $name, $path);
		return $db;
	}
	
	function &newSchemaObj(&$db, $name)
	{
		$schema =& new fSQLStandardSchema($db, $name);
		return $schema;
	}
	
	function &newTableObj(&$schema, $name)
	{
		$table =& new fSQLStandardTable($schema, $name);
		return $table;
	}
	
	function &newTableDefObj(&$schema, $table_name)
	{
		$def =& new fSQLStandardTableDef($schema, $table_name);
		return $def;
	}
	
	function &newViewObj(&$schema, $name)
	{
		$view =& new fSQLStandardView($schema, $name);
		return $view;
	}
}

/**
 * 
 */
class fSQLStandardDatabase extends fSQLDatabase
{
	/*
	 * Creates directory|directories using path attribute and defines a schema 
	 * @return bool false if create directory or define schema fails
	 */
	function create()
	{
		$path = create_directory($this->path, 'database', $this->environment);
		if($path !== false) {
			$this->path = $path;
			return parent::create();
		}
		else
			return false;
	}
	
	/*
	 * Drops all schemas in this database and closes database.
	 * @return bool always true
	 */
	function drop()
	{
		foreach(array_keys($this->schemas) as $schema_name)
			$this->schemas[$schema_name]->drop();
		$this->close();
		return true;
	}
}

/**
 * 
 */
class fSQLStandardSchema extends fSQLSchema
{
	var $path;
	var $loadedTables = array();

	/*
	 * Class constructor calls parent constructor with arguments. This method 
 	 * sets path attribute according to name. If name is not public, it is
	 * included in path.
	 * @param 
	 * @param string $name
	 */
	function fSQLStandardSchema(&$database, $name)
	{
		parent::fSQLSchema($database, $name);
		$this->path = $name !== 'public' ? $database->getPath().$name.'/' : $database->getPath();
	}
	
	function create()
	{
		if($this->name !== 'public') {
			$path = create_directory($this->path, 'schema', $this->database->getEnvironment());
			if($path !== false) {
				$this->path = $path;
			}
			else
				return false;
		}
		
		return true;
	}
	
	/*
	 * Calls parent close method and unsets attributes.
	 */
	function close()
	{
		parent::close();
		unset($this->loadedTables);
	}
	
	/*
	 * Returns schema path.
	 */
	function getPath()
	{
		return $this->path;
	}

	/*
	 * Creates table whose name and columns are given as arguments. If table
	 * is temporary, it is created as fSQLMemoryTable table and added to 
	 * loadedTables array.
	 * @param string $table_name
	 * @param string $columns (???)
	 * @param bool $temporary
	 * @return fSQLTable
	 */
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table =& parent::createTable($table_name, $columns, $temporary);
		
		// If temporary table, store it in loadedTables now.
		// For memory's sake, regular tables aren't stored in
		// loadedTables until the first time getTable is called on them.
		if($table !== false && $temporary)
			$this->loadedTables[$table_name] =& $table;
		
		return $table;
	}
	
	/*
	 * Finds and returns table. 
	 * @param string $table_name
	 * @return fSQLTable
	 */
	function &getTable($table_name)
	{
		$table = false;
		
		if(!isset($this->loadedTables[$table_name]))
		{
			if($this->tableExists($table_name))
			{
				$path_prefix = $this->path.$table_name;
		
				$driver =& $this->database->getDriver();
				if(file_exists($path_prefix.'.data.cgi')) {
					$table =& $driver->newTableObj($this, $table_name);
				} else if(file_exists($path_prefix.'.view.cgi')) {
					$table =& $driver->newViewObj($this, $table_name);
					$table->execute();
				}
				
				$this->loadedTables[$table_name] =& $table;
				unset($table);
			}
			else
				return $table;
		}
		
		return $this->loadedTables[$table_name];
	}

	/*
	 * Checks whether table exists.
	 * @param string $table_name
	 * @return bool
	 */
	function tableExists($table_name)
	{
		return in_array($table_name, $this->listTables());
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * @return array the table names
	 */
	function listTables()
	{
		$dir = opendir($this->path);

		$tables = array();

		// Look for all files in path. If file name has the form 
		// filename.columns.cgi, add filename to $tables.
		while (false !== ($file = readdir($dir))) {
			if ($file !== '.' && $file !== '..' && !is_dir($file)) {
				if(substr($file, -12) === '.columns.cgi') {
					$tables[] = substr($file, 0, -12);
				}
			}
		}
		
		closedir($dir);
		
		return $tables;
	}
	
	/**
	 * Renames (and copies/moves if necessary) the table. If table is temporary
	 * simply table renamed and added to new database. If table is not temporary
	 * a new table is created in new database with the same properties as the
	 * old table has. Old table's dataFile and dataLockFile are copied to
	 * new directory. In all cases, old table is deleted from database. 
	 * @param string $old_table_name
	 * @param string $new_table_name
	 * @param fSQLDatabase $new_db
	 * @return bool 
	 */
	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		$oldTable =& $this->getTable($old_table_name);
		if($oldTable !== false) {
			if(!$oldTable->temporary()) {
				$newTable =& $new_db->createTable($oldTable->getColumns());
				copy($oldTable->dataFile->getPath(), $newTable->dataFile->getPath());
				copy($oldTable->dataLockFile->getPath(), $newTable->dataLockFile->getPath());
				$this->dropTable($old_table_name);
			} else {
				$new_db->loadedTables[$new_table_name] =& $oldTable;
				$oldTable->rename($new_table_name);
				unset($this->loadedTables[$old_table_name]);
			}

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Drops table and unsets related attributes.
	 * @param string $table_name
	 * @return bool
	 */
	function dropTable($table_name)
	{
		$table =& $this->getTable($table_name);
		if($table !== false) {
			$table->drop();
			unset($this->loadedTables[$table_name]);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Copies table to a destination path. 
	 * @param string $name
	 * @param string $src_path 
	 * @param string $dst_path 
	 */
	function copyTable($name, $src_path, $dest_path)
	{
		copy($src_path.$name.'columns.cgi', $dest_path.$name.'columns.cgi');
		copy($src_path.$name.'data.cgi', $dest_path.$name.'data.cgi');
	}
}

class fSQLStandardTableDef extends fSQLTableDef
{
	var $keys = array();
	var $columns = null;
	var $columnsFile;
	var $columnsLockFile;
	var $lock = null;
	var $readFunction = null;
	
	function fSQLStandardTableDef(&$schema, $table_name)
	{
		$path = $schema->getPath().$table_name.'.columns';
		$this->columnsLockFile = new fSQLMicrotimeLockFile($path.'.lock.cgi');
		$this->columnsFile = new fSQLFile($path.'.cgi');
	}
	
	function close()
	{
		if(isset($this->columnsLockFile))
			$this->columnsLockFile->close();
		if(isset($this->columnsFile))
			$this->columnsFile->close();
		
		unset(
			$this->columns,
			$this->columnsFile,
			$this->columnsLockFile,
			$this->lock,
			$this->readFunction
		);
	}
	
	function addKey($name, $type, $columns, $engine, $fileName)
	{
		return false;
	}
	
	function drop()
	{
		if($this->lock === null)
		{
			$this->columnsFile->drop();
			$this->columnsLockFile->drop();
			$this->close();
			return true;
		}
		else
			return false;
	}

	function getReadFunction()
	{
		if($this->readFunction === null)
			$columns = $this->getColumns();
		return $this->readFunction;
	}
	
	function getKeyNames()
	{
		$this->getColumns();
		return array_keys($this->keys);
	}
	
	function getKeysInfo()
	{
		$this->getColumns();
		return $this->keys;
	}
	
	function _buildReadWriteFuncs()
	{
		return false;
	}
	
	function setColumns($columns)
	{
		return false;
	}
	
	function isReadLocked()
	{
		return $this->lock === 'r';
	}

	function readLock()
	{
		$success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead();
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
		$success = $this->columnsLockFile->acquireWrite() && $this->columnsFile->acquireWrite();
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
		}
		else if($this->lock === 'w')
		{
			$this->columnsLockFile->releaseWrite();
			$this->columnsFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

/**
 * Class for the standard fSQL tables that are saved to the filesystem.
 */
class fSQLStandardTable extends fSQLTable
{
	var $rcursor = null;
	var $wcursor = null;
	var $entries = null;
	var $loadedKeys = array();
	var $dataLockFile;
	var $dataFile;
	var $lock = null;
	
	function fSQLStandardTable(&$schema, $name)
	{
		parent::fSQLTable($schema, $name);
		$data_path = $schema->getPath().$name.'.data';
		$this->dataLockFile = new fSQLMicrotimeLockFile($data_path.'.lock.cgi');
		$this->dataFile = new fSQLFile($data_path.'.cgi');
	}
	
	function create($columnDefs)
	{
		return false;
	}
	
	function close()
	{
		if($this->rcursor !== null)
			$this->rcursor->close();
		if($this->wcursor !== null)
			$this->wcursor->close();
		if(!empty($this->loadedKeys))
		{
			foreach(array_keys($this->loadedKeys) as $k)
				$this->loadedKeys[$k]->close();
		}
		$this->dataLockFile->close();
		$this->dataFile->close();
		
		unset(
			$this->loadedKeys,
			$this->entries,
			$this->dataFile,
			$this->dataLockFile,
			$this->lock,
			$this->readFunction
		);
		
		parent::close();
	}

	function temporary() {
		return false;
	}
	
	function &getCursor()
	{
		$this->_loadEntries();

		if($this->rcursor === null)
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
		return false;
	}
	
	function getKeyNames()
	{
		return $this->definition->getKeyNames();
	}
	
	function getKeys()
	{
		$keys = array();
		$keyNames = $this->definition->getKeyNames();
		foreach($keyNames as $keyName)
			$keys[] =& $this->getKey($keyName);
		return $keys;
	}
	
	function &getKey($key_name)
	{
		if(!isset($this->loadedKeys[$key_name]))
		{
			$key = false;
			$allKeys = $this->definition->getKeysInfo();
			if(isset($allKeys[$key_name]))
			{
				$keydata = $allKeys[$key_name];
				if($keydata['engine'] === 'HASH_FILE')
				{
					$key =& new fSQLDefaultKey($this->schema->getPath().$keydata['file']);
					$key->load();
					$this->loadedKeys[$key_name] =& $key;
				}
				else if($keydata['engine'] === 'MEM')
				{
					$key =& new fSQLMemoryKey($keydata['type']);
					$key->create($keydata['columns']);
					$this->loadedKeys[$key_name] =& $key;
				}
			}
			return $key;
		}
		else
			return $this->loadedKeys[$key_name];
	}
	
	function _closeLoadedKeys()
	{
		if(!empty($this->loadedKeys))
		{
			foreach(array_keys($this->loadedKeys) as $k)
				$this->loadedKeys[$k]->close();
			$this->loadedKeys = array();
		}
	}
	
	function rollback()
	{
		// force re-read of table data next time _loadEntries() is called
		$this->queryLockFile->reset();
		
		// close the write cursor
		if(isset($this->wcursor))
		{
			$this->wcursor->close();
			$this->wcursor = null;	
		}
		
		// close all loaded keys forcing refresh of them as well
		$this->_closeLoadedKeys();
	}

	function drop()
	{
		if($this->lock === null)
		{
			$this->definition->drop();
			$this->dataFile->drop();
			$this->dataLockFile->drop();
			$this->close();
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

class fSQLStandardView extends fSQLView
{
	var $rcursor = null;
	var $queryLockFile;
	var $queryFile;
	var $lock = null;
	
	function fSQLStandardView(&$schema, $name)
	{
		parent::fSQLView($schema, $name);
		$query_path = $schema->getName().$name.'.view';
		$this->queryLockFile =& new fSQLMicrotimeLockFile($query_path.'.lock.cgi');
		$this->queryFile =& new fSQLFile($query_path.'.cgi');
	}
	
	function close()
	{
		if($this->rcursor !== null)
			$this->rcursor->close();
		$this->queryFile->close();
		$this->queryLockFile->close();
		
		unset($this->rcursor, $this->queryFile, $this->queryLockFile, $this->lock);
		parent::close();
	}
	
	function define($query, $columns)
	{
		// create the view lock
		$this->queryLockFile->write();
		
		// create the view file
		$this->queryFile->acquireWrite();
		$definition = $this->queryFile->getHandle();
		ftruncate($definition, 0);
		fwrite($definition, $query);
		
		$this->queryFile->releaseWrite();	
		
		$this->setQuery($query);
		$this->definition->setColumns($columns);
		$this->execute();
	}
	
	function drop()
	{
		if($this->lock === null)
		{
			$this->definition->drop();
			$this->queryFile->drop();
			$this->queryLockFile->drop();
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
	
	function getEntries() {
		return $this->entries;
	}
	
	function &getCursor()
	{
		if($this->rcursor === null)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}
	
	function _loadView()
	{
		$this->queryLockFile->acquireRead();
		if($this->queryLockFile->wasModified())
		{
			$this->queryLockFile->accept();

			$this->queryFile->acquireRead();
			$dataHandle = $this->queryFile->getHandle();

			$this->query = file_read_line($dataHandle);

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
		$success = $this->definition->readLock() && $this->queryLockFile->acquireRead() && $this->queryFile->acquireRead();
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
		$success = $this->definition->writeLock() && $this->queryLockFile->acquireWrite() && $this->queryFile->acquireWrite();
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
			$this->queryLockFile->releaseRead();
			$this->queryFile->releaseRead();
		}
		else if($this->lock === 'w')
		{
			$this->definition->unlock();
			$this->queryLockFile->releaseWrite();
			$this->queryFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

class fSQLMicrotimeLockFile extends fSQLFile
{
	var $loadTime = null;
	var $lastReadStamp = null;
	
	function accept()
	{
		$this->loadTime = $this->lastReadStamp;
	}
	
	function close()
	{
		unset($this->loadTime, $this->lastReadStamp);
		parent::close();
	}
	
	function reset()
	{
		$this->loadTime = null;
		$this->lastReadStamp = null;
		return true;
	}
	
	function wasModified()
	{
		$this->acquireRead();
		
		$this->lastReadStamp = fread($this->handle, 20);
		$modified = $this->loadTime === null || $this->loadTime < $this->lastReadStamp;
		
		$this->releaseRead();
		
		return $modified;
	}
	
	function wasNotModified()
	{
		$this->acquireRead();
		
		$this->lastReadStamp = fread($this->handle, 20);
		$modified = $this->loadTime === null || $this->loadTime >= $this->lastReadStamp;
		
		$this->releaseRead();
		
		return $modified;
	}
	
	function write()
	{
		$this->acquireWrite();
	
		list($msec, $sec) = explode(' ', microtime());
		$this->loadTime = $sec.$msec;
		ftruncate($this->handle, 0);
		fwrite($this->handle, $this->loadTime);
		
		$this->releaseWrite();
	}
}

?>