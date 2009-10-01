<?php

class fSQLSchema
{
	var $name = null;
	var $path = null;
	var $database = null;
	
	function create()
	{
		return false;
	}
	
	function drop()
	{
		return true;
	}
	
	function close()
	{
		unset($this->name, $this->path, $this->database);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = null;
		return $table;
	}
	
	function &createView($view_name, $query, $columns = null)
	{
		$view = null;
		return $view;
	}
	
	function &getDatabase()
	{
		return $this->database;
	}
	
	function getName()
	{
		return $this->name;
	}

	function getPath()
	{
		return $this->path;
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

class fSQLMemorySchema extends fSQLSchema
{
	var $tables = array();
	
	function fSQLMemorySchema(&$database, $name)
	{
		$this->name = $name;
		$this->database =& $database;
		$this->path = FSQL_MEMORY_DB_PATH;
	}

	function create()
	{
		return true;
	}
	
	function close()
	{
		parent::close();
		unset($this->tables);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table =& new fSQLTemporaryTable($table_name, $this);
		$this->tables[$table_name] =& $table;
		//$table =& $this->tables[$table_name];
		$table->create($columns);
		return $table;
	}
	
	function &createView($view_name, $query, $columns = null)
	{
		$table =& new fSQLTemporaryView($view_name, $this);
		$this->tables[$view_name] =& $table;
		$table->define($query, $columns);
		return $this->tables[$view_name];
	}
	
	function &getTable($table_name)
	{
		$rel = false;
		if(isset($this->tables[$table_name]))
		{
			$rel =& $this->tables[$table_name];
			if(is_a($rel, 'fSQLView')) {
				$this->tables[$table_name]->execute();
			}
			return $this->tables[$table_name];
		}
	
		return $rel;
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
		if($oldTable !== false) {
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
		if($table !== false) {
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

class fSQLStandardSchema extends fSQLSchema
{
	var $loadedTables = array();

	function fSQLStandardSchema(&$database, $name)
	{
		$this->name = $name;
		$this->database =& $database;
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
	
	function close()
	{
		parent::close();
		unset($this->loadedTables);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = false;
		
		if(!$temporary) {
			$table =& new fSQLStandardTable($table_name, $this);
		} else {
			$table =& new fSQLTemporaryTable($table_name, $this);
			$this->loadedTables[$table_name] =& $table;
		}

		$table->create($columns);
		
		return $table;
	}
	
	function &createView($view_name, $query, $columns = null)
	{
		$table =& new fSQLStandardView($view_name, $this);
		$this->tables[$view_name] =& $table;
		$table->define($query, $columns);
		return $this->tables[$view_name];
	}
	
	function &getTable($table_name)
	{
		$table = false;
		
		if(!isset($this->loadedTables[$table_name]))
		{
			if($this->tableExists($table_name))
			{
				$path_prefix = $this->path.$table_name;
		
				if(file_exists($path_prefix.'.data.cgi')) {
					$table =& new fSQLStandardTable($table_name, $this);
				} else if(file_exists($path_prefix.'.view.cgi')) {
					$table =& new fSQLStandardView($table_name, $this);
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
	
	function tableExists($table_name)
	{
		return in_array($table_name, $this->listTables());
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * 
	 * @return array the table names
	 */
	function listTables()
	{
		$dir = opendir($this->path);

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
	
	function dropTable($table_name)
	{
		$table =& $this->getTable($table_name);
		if($table !== false) {
			$table->drop();			
			$table = null;
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

class fSQLMasterSchema extends fSQLMemorySchema
{
	function fSQLMasterSchema(&$database, $name)
	{
		parent::fSQLMemorySchema($database, $name);
	}
	
	function create()
	{
		$databasesTable =& $this->createTable('databases', array(
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'path' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null)
			)
		);
		
		$schemasTable =& $this->createTable('schemas', array(
				'database' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'path' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null)
			)
		);
		
		$tablesTable =& $this->createTable('tables', array(
				'database' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'schema' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'type' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null)
			)
		);
		
		$columnsTable =& $this->createTable('columns', array(
				'database' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'schema' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'table' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'p', 'restraint' => null),
				'type' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null),
				'default' => array('type' => FSQL_TYPE_STRING, 'default' => null, 'null' => true, 'auto' => false, 'key' => 'n', 'restraint' => null),
				'key' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null),
				'nullable' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null),
				'auto' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'auto' => false, 'key' => 'n', 'restraint' => null)
			)
		);
		
		return true;
	}
	
	function addDatabase(&$database)
	{
		$dbTable =& $this->getTable('databases');
		$dbTable->getWriteCursor()->appendRow(array($database->getName(), $database->getPath()));
	}
	
	function addSchema(&$schema)
	{
		$schemaTable =& $this->getTable('schemas');
		$schemaTable->getWriteCursor()->appendRow(array($schema->getDatabase()->getName(), $schema->getName(), $schema->getPath()));
		
		foreach($schema->listTables() as $table_name)
		{
			$table =& $schema->getTable($table_name);
			$this->addTable($table);
		}
	}
	
	function addTable(&$table)
	{
		$schema =& $table->getSchema();
		$tablesTable =& $this->getTable('tables');
		if(is_a($table, 'fSQLView'))
			$type = 'VIEW';
		else if($table->temporary()) {
			$type = 'LOCAL TEMPORARY';
		} else {
			$type = 'BASE TABLE';
		}
		$tablesTable->getWriteCursor()->appendRow(array($schema->getDatabase()->getName(), $schema->getName(), $table->getName(), $type));
		
		$this->addColumns($table);
	}
	
	function addColumns(&$table)
	{
		$schema =& $table->getSchema();
		$database =& $schema->getDatabase();
		$environment =& $database->getEnvironment();
		
		$schema_name = $schema->getName();
		$table_name = $table->getName();
		$db_name = $database->getName();
		
		$columnsTable =& $this->getTable('columns');
		$columnsCursor =& $columnsTable->getWriteCursor();
		
		foreach($table->getColumns() as $col_name => $columnDef)
		{
			$type = $environment->_typecode_to_name($columnDef['type']);
			$nullable = $columnDef['type'] ? 'YES' : 'NO';
			$auto = $columnDef['auto'] ? 'YES' : 'NO';
			switch($columnDef['key'])
			{
				case 'p': $key = 'PRIMARY';	break;
				case 'u': $key = 'UNIQUE';	break;
				case 'k': $key = 'KEY';		break;
				default:  $key = '';  break;
			}
			$default = $columnDef['default'] !== null ? (string) $columnDef['default'] : null;
			$columnsCursor->appendRow(array($db_name, $schema_name, $table_name, $col_name, $type, $default, $key, $nullable, $auto));
		}
	}
	
	function removeDatabase(&$database)
	{	
		$db_name = $database->getName();
		
		$databasesCursor =& $this->getTable('databases')->getWriteCursor();
		for($databasesCursor->first(); !$databasesCursor->isDone(); $databasesCursor->next())
		{
			list($curr_db_name, ) = $databasesCursor->getRow();
			if($curr_db_name === $db_name) {
				$databasesCursor->deleteRow();
				break;
			}
		}
		
		foreach($database->listSchemas() as $schema_name)
		{
			$schema =& $database->getSchema($schema_name);
			$this->removeSchema($schema);
		}
	}
	
	function removeSchema(&$schema)
	{	
		$db_name = $schema->getDatabase()->getName();
		$schema_name = $schema->getName();
		
		$schemasCursor =& $this->getTable('schemas')->getWriteCursor();
		for($schemasCursor->first(); !$schemasCursor->isDone(); $schemasCursor->next())
		{
			list($curr_db_name, $curr_schema_name, ) = $schemasCursor->getRow();
			if($curr_db_name === $db_name && $curr_schema_name === $schema_name) {
				$schemasCursor->deleteRow();
				break;
			}
		}
		
		foreach($schema->listTables() as $table_name)
		{
			$table =& $schema->getTable($table_name);
			$this->removeTable($table);
		}
	}
	
	function removeTable(&$table)
	{
		$schema =& $table->getSchema();
		
		$db_name = $schema->getDatabase()->getName();
		$schema_name = $schema->getName();
		$table_name = $table->getName();
		
		$tablesTable =& $this->getTable('tables');
		$tablesCursor =& $tablesTable->getWriteCursor();
		for($tablesCursor->first(); !$tablesCursor->isDone(); $tablesCursor->next())
		{
			list($curr_db_name, $curr_schema_name, $curr_table_name, ) = $tablesCursor->getRow();
			if($curr_db_name === $db_name && $curr_schema_name === $schema_name && $curr_table_name === $table_name) {
				$tablesCursor->deleteRow();
				break;
			}
		}
		
		$this->removeColumns($table);
	}
	
	function removeColumns(&$table)
	{
		$schema =& $table->getSchema();
		
		$db_name = $schema->getDatabase()->getName();
		$schema_name = $schema->getName();
		$table_name = $table->getName();
		
		$columnsCursor =& $this->getTable('columns')->getWriteCursor();
		$columnsCursor->first();
		while(!$columnsCursor->isDone())
		{
			list($curr_db_name, $curr_schema_name, $curr_table_name, ) = $columnsCursor->getRow();
			if($curr_db_name === $db_name && $curr_schema_name === $schema_name && $curr_table_name === $table_name)
				$columnsCursor->deleteRow();
			else
				$columnsCursor->next();
		}
	}
}

?>