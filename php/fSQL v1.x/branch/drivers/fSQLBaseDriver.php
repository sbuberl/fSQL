<?php

class fSQLDriver
{
	function &defineDatabase(&$environment, $name)
	{
		return false;
	}	
}

class fSQLDatabase
{
	var $environment;
	var $name;
	var $path;
	var $schemas = array();

	function fSQLDatabase(&$environment, $name, $path)
	{
		$this->environment =& $environment;
		$this->name = $name;
		$this->path = $path;
	}
	
	function create()
	{
		return false;
	}
	
	function close()
	{
		foreach(array_keys($this->schemas) as $schema_name)
			$this->schemas[$schema_name]->close();

		unset($this->name, $this->path, $this->schemas, $this->environment);
	}
	
	function &getEnvironment()
	{
		return $this->environment;
	}
	
	function getName()
	{
		return $this->name;
	}
	
	function getPath()
	{
		return $this->path;
	}
	
	function &_createSchema($name)
	{
		$schema = false;
		return $schema;
	}
	
	function &defineSchema($name)
	{
		$schema = false;
		
		if(!isset($this->schemas[$name]))
		{
			$this->schemas[$name] =& $this->_createSchema($name);
			if($this->schemas[$name]->create())
				return $this->schemas[$name];
		}
		
		return $schema;
	}
	
	function &getSchema($name)
	{
		$schema = false;
		if(isset($this->schemas[$name]))
		{
			$schema =& $this->schemas[$name];
		}
		
		return $schema;
	}
	
	function dropSchema($name)
	{
		$schema = false;
		if(isset($this->schemas[$name]))
		{
			$this->schemas[$name]->drop();
			unset($this->schemas[$name]);
		}
		
		return $schema;
	}
	
	function listSchemas()
	{
		return array_keys($this->schemas);
	}
	
	function drop()
	{
		foreach(array_keys($this->schemas) as $schema_name)
			$this->schemas[$schema_name]->drop();
		$this->close();
		return true;
	}
}

class fSQLSchema
{
	var $name = null;
	var $database = null;
	
	function fSQLSchema(&$database, $name)
	{
		$this->database =& $database;
		$this->name = $name;
	}
	
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
		unset($this->name, $this->database);
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

class fSQLTableDef
{	
	function close() { return false; }
	function drop() { return false; }

	function getColumnNames() {
		$columns = $this->getColumns();
		return $columns !== false ? array_keys($columns) : false;
	}
	function getColumns() { return false; }
	function setColumns($columns) { }
	
	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
}

/*
 * Base class for fSQL tables
 */
class fSQLTable
{
	var $name;
	var $definition = null;
	var $schema;

	function fSQLTable($name, &$schema)
	{
		$this->name = $name;
		$this->schema =& $schema;
	}

	function getName()
	{
		return $this->name;
	}
	
	function getFullName()
	{
		$db = $this->schema->getDatabase();
		return $db->getName().'.'.$this->schema->getName().'.'.$this->name;
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
		if(isset($this->definition))
			$this->definition->close();
		unset($this->definition, $this->schema, $this->name);
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

class fSQLView extends fSQLTable
{
	var $query = null;
	var $columns = null;
	var $entries = null;
	
	function close()
	{
		parent::close();
		unset($this->query, $this->columns, $this->entries);
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
		$database =& $this->schema->getDatabase();
		$env =& $database->getEnvironment();
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

?>