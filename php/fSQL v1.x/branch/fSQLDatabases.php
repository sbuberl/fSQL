<?php

class fSQLDatabase
{
	var $name = null;
	var $path_to_db = null;

	function close()
	{
		unset($this->name, $this->path_to_db);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		return null;
	}

	function getPath()
	{
		return $this->path_to_db;
	}
	
	function &getTable($table_name)
	{
		$table = null;
		return $table;
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * 
	 * @return array the table names
	 */
	function listTables()
	{
		return array();
	}
	
	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		return false;
	}
	
	function dropTable($table_name)
	{
		return false;
	}
	
	function copyTable($name, $src_path, $dest_path)
	{
		return false;
	}
}

class fSQLMemoryDatabase extends fSQLDatabase
{
	var $tables = array();

	function close()
	{
		unset($this->name, $this->path_to_db, $this->tables);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = new fSQLTemporaryTable($table_name, $this);
		$this->tables[$table_name] =& $table;
		$table->create($columns);
		return $table;
	}
	
	function &getTable($table_name)
	{
		if(!isset($this->tables[$table_name])) {
			$table = new fSQLStandardTable($table_name, $this);
			$this->tables[$table_name] =& $table;
		}
		
		return $this->tables[$table_name];
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * 
	 * @return array the table names
	 */
	function listTables()
	{
		return array_keys($this->tables);
	}
	
	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		$oldTable =& $this->getTable($old_table_name);
		if($oldTable->exists()) {
			$new_db->tables[$new_table_name] =& $oldTable;
			$oldTable->rename($new_table_name);
			unset($this->tables[$old_table_name]);
			return true;
		} else {
			return false;
		}
	}
	
	function dropTable($table_name)
	{
		$table =& $this->getTable($table_name);
		if($table->exists()) {
			$table->drop();			
			$table = NULL;
			unset($this->tables[$table_name]);
			unset($table);
			return true;
		} else {
			return false;
		}
	}
	
	function copyTable($name, $src_path, $dest_path)
	{
		copy($src_path.$name.'columns.cgi', $dest_path.$name.'columns.cgi');
		copy($src_path.$name.'data.cgi', $dest_path.$name.'data.cgi');
	}
}

class fSQLStandardDatabase extends fSQLDatabase
{
	var $loadedTables = array();

	function close()
	{
		unset($this->name, $this->path_to_db, $this->loadedTables);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = NULL;
		
		if(!$temporary) {
			$table = new fSQLStandardTable($table_name, $this);
		} else {
			$table = new fSQLTemporaryTable($table_name, $this);
			$this->loadedTables[$table_name] =& $table;
		}

		$table->create($columns);
		
		return $table;
	}
	
	function &getTable($table_name)
	{
		if(!isset($this->loadedTables[$table_name])) {
			$table = new fSQLStandardTable($table_name, $this);
			$this->loadedTables[$table_name] = $table;
			unset($table);
		}
		
		return $this->loadedTables[$table_name];
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * 
	 * @return array the table names
	 */
	function listTables()
	{
		$dir = opendir($this->path_to_db);

		$tables = array();
		while (false !== ($file = readdir($dir))) {
			if ($file !== '.' && $file !== '..' && !is_dir($file)) {
				if(substr($file, -12) == '.columns.cgi') {
					$tables[] = substr($file, 0, -12);
				}
			}
		}
		
		closedir($dir);
		
		return $tables;
	}
	
	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		$oldTable =& $this->getTable($old_table_name);
		if($oldTable->exists()) {
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
	
	function dropTable($table_name)
	{
		$table =& $this->getTable($table_name);
		if($table->exists()) {
			$table->drop();			
			$table = NULL;
			unset($this->loadedTables[$table_name]);
			unset($table);

			return true;
		} else {
			return false;
		}
	}
	
	function copyTable($name, $src_path, $dest_path)
	{
		copy($src_path.$name.'columns.cgi', $dest_path.$name.'columns.cgi');
		copy($src_path.$name.'data.cgi', $dest_path.$name.'data.cgi');
	}
}

?>