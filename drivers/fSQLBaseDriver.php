<?php
/**
 * Driver module base classes file
 * 
 * This file is describes base classes for drivers module. 
 * @author Kaja Fumei <kaja.fumei@gmail.com>
 * @version 
 */

/**
 * Base driver class.  Also a factory class creating
 * objcts of the right type. Has no attribute.
 */
class fSQLDriver
{
	function &defineDatabase(&$environment, $name, $args)
	{
		return false;
	}
	
	function isAbstract()
	{
		return true;
	}
	
	function &_undefinedNew() { $o = false; return $o; }
	
	function &newSchemaObj(&$db, $name) { return fSQLDriver::_undefinedNew(); }
	
	function &newTableObj(&$schema, $name) { return fSQLDriver::_undefinedNew(); }
	function &newTempTableObj(&$schema, $name)
	{
		$table =& new fSQLMemoryTable($schema, $name);
		return $table;
	}
	
	function &newTableDefObj(&$schema, $table_name) { return fSQLDriver::_undefinedNew(); }
	
	function &newViewObj(&$schema, $name) { return fSQLDriver::_undefinedNew(); }
	function &newTempViewObj(&$schema, $name)
	{
		$view =& new fSQLMemoryView($schema, $name);
		return $view;
	}
}

/**
 * Base database class. This class provides basic properties for any kind of 
 * database.
 */
class fSQLDatabase
{
	var $driver;
	
        /**
        * The environment which this database is created. 
        * @var fSQLEnvironment
        */
	var $environment;

        /**
        * The unique(?) name of the database. 
        * @var string
        */
	var $name;

        /**
        * The database path, this may be a filesystem path or a keyword. Subclasses
	* implements actions taken according to path definition. 
        * @var string
        */
	var $path;

        /**
        * The array of schemas defined in this database.  
        * @var array
        */
	var $schemas = array();

        /**
        * Class constructor. Sets object attributes environment, name and path.
        * @param fSQLEnvironment $environment 
        * @param string $name 
        * @param string $path 
        */
	function fSQLDatabase(&$driver, &$environment, $name, $path)
	{
		$this->driver =& $driver;
		$this->environment =& $environment;
		$this->name = $name;
		$this->path = $path;
	}

        /**
        * Database creation done in this method. This class does not implement 	
	* method, subclasses will do.
        */
	function create()
	{
		return $this->defineSchema('public') !== false;
	}
	
        /**
        * All schemas closed and class attributes are unsetted. 
        */
	function close()
	{
		foreach(array_keys($this->schemas) as $schema_name)
			$this->schemas[$schema_name]->close();

		unset($this->name, $this->path, $this->schemas, $this->environment);
	}
	
        /**
        * Returns the environment which created the database object.
	* @return fSQLEnvironment parent environment
        */
	function &getEnvironment()
	{
		return $this->environment;
	}
	
	function &getDriver()
	{
		return $this->driver;
	}
	
        /**
        * Returns database name  
	* @return string database name
        */
	function getName()
	{
		return $this->name;
	}
	
        /**
        * Returns database path  
	* @return string database path
        */
	function getPath()
	{
		return $this->path;
	}
	
	/**
        * Creates a schema and inserts it to schemas array. 
	* @param string $name
	* @return fSQLSchema|bool new schema or false
        */
	function &defineSchema($name)
	{
		$schema = false;
		
		if(!isset($this->schemas[$name]))
		{
			$this->schemas[$name] =& $this->driver->newSchemaObj($this, $name);
			if($this->schemas[$name]->create())
				return $this->schemas[$name];
			else
				unset($this->schemas[$name]);
		}
		
		return $schema;
	}

	/**
        * Returns schema if defined.  
	* @param string $name
	* @return fSQLSchema|bool schema if defined else false
        */
	function &getSchema($name)
	{
		$schema = false;
		if(isset($this->schemas[$name]))
		{
			$schema =& $this->schemas[$name];
		}
		
		return $schema;
	}
	
	/**
        * Method drops schema and deletes from schemas list.  
	* @param string $name
	* @return bool always false (???)
        */
	function dropSchema($name)
	{
		$schema = false;

		// drop and unset schema if defined
		if(isset($this->schemas[$name]))
		{
			$this->schemas[$name]->drop();
			unset($this->schemas[$name]);
		}
		
		return $schema;
	}
	
	/**
        * Returns an array of schema names defined in this database.
	* @return array
        */
	function listSchemas()
	{
		return array_keys($this->schemas);
	}

	/**
        * Drops all schemas and closes database. 
	* @return bool always true
        */
	function drop()
	{
		// traverse schemas array and drop each
		foreach(array_keys($this->schemas) as $schema_name)
			$this->schemas[$schema_name]->drop();

		// close database
		$this->close();
		return true;
	}
}

/**
 * Base schema class. This class provides basic properties for any kind of 
 * schema. Schemas are created inside of a database ( i.e each schema is defined
 * under a database).
 */
class fSQLSchema
{
        /**
        * The schema name. 
        * @var string
        */
	var $name = null;

	/**
        * The database which schema is defined. 
        * @var fSQLDatabase
        */
	var $database = null;

        /**
        * Class constructor. Sets object attributes environment, database and name.
        * @param fSQLDatabase $database 
        * @param string $name 
        */
	function fSQLSchema(&$database, $name)
	{
		$this->database =& $database;
		$this->name = $name;
	}
	
        /**
        * Creates schema. This class does not implement method, subclasses will do.
	* @return bool always true
        */
	function create()
	{
		return true;
	}
	
        /**
        * Drops schema. This class does not implement method, subclasses will do.
	* @return bool always true
        */
	function drop()
	{
		$this->close();
		return true;
	}

        /**
        * Closes schema as unsetting attributes.
        */
	function close()
	{
		unset($this->name, $this->database);
	}
	
	function &createKey($name, $type, $columns, &$table)
	{
		$key = null;
		return $key;
	}
	
        /**
        * Creates table.
	* @param string $table_name
	* @param string $columns
	* @param bool $temporary
	* @return fSQLTable|bool created table or false
        */
	function &createTable($table_name, $columns, $temporary = false)
	{
		$driver =& $this->database->getDriver();
		if($temporary)
			$table =& $driver->newTempTableObj($this, $table_name);
		else
			$table =& $driver->newTableObj($this, $table_name);
		$table->create($columns);
		return $table;
	}
	
        /**
        * Creates view.
	* @param string $view_name
	* @param string $query
	* @param string $columns
	* @return fSQLView fSQLView|bool created view or false
        */
	function &createView($view_name, $query, $columns = null)
	{
		$driver =& $this->database->getDriver();
		$view =& $driver->newTableObj($this, $view_name);
		$view->define($query, $columns);
		return $view;
	}
	
	/**
	* Returns parent database.
	* @return fSQLDatabase
	*/
	function &getDatabase()
	{
		return $this->database;
	}
	
	/**
	* Returns schema name.
	* @return string 
	*/
	function getName()
	{
		return $this->name;
	}
	
	/**
	* Returns a table whose name is $table_name
	* @param strint $table_name
	* @return fSQLDatabase
	*/
	function &getTable($table_name)
	{
		$table = null;
		return $table;
	}
	
	/**
	* Returns an array of names of all the tables in the database
	* @return array the table names
	*/
	function listTables()
	{
		return array();
	}

        /**
        * Renames a table. This class does not implement method, subclasses will do.
	* @return bool always false
        */
	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		return false;
	}

        /**
        * Drops table. This class does not implement method, subclasses will do.
	* @return bool always false
        */
	function dropTable($table_name)
	{
		return false;
	}
	
        /**
        * Copies a table. This class does not implement method, subclasses will do.
	* @return bool always false
        */
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
	var $definition;
	var $schema;

	function fSQLTable(&$schema, $name)
	{
		$this->name = $name;
		$this->schema =& $schema;
		$db =& $schema->getDatabase();
		$driver =& $db->getDriver();
		$this->definition =& $driver->newTableDefObj($schema, $name);
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
	
	function drop() { return $this->close(); }
	function temporary() { return false; }
	
	function nextValueFor($column)
	{
		$columns = $this->definition->getColumns();
		if(!isset($columns[$column]))
			return false;
		
		if($columns[$column]['identity'] !== null)
		{
			list(, $start, $increment, $min, $max, $canCycle) = $columns[$column]['identity'];
			
			$cycled = false;
			if($increment > 0 && $start > $max)
			{
				$cycled = true;
			}
			else if($increment < 0 && $start < $min)
			{
				$cycled = true;	
			}
			
			if($cycled && !$canCycle)
				return false;
				
			$columns[$column]['identity'][1] += $increment;
			
			$this->definition->setColumns($columns);
			
			return $start;
		}
		else
			return false;
	}
	
	function getKeyNames() { return false; }
	function getKeys() { return false ; }
	
	function &getCursor() { return null; }
	function &getWriteCursor() { return null; }
	function getEntries() { return null; }
	
	function commit() { return false; }
	function rollback() { return false; }
	
	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
}

/*
 * Base class for fSQL views. A view is a virtual table based on the result-set 
 * of an SQL statement. A view contains rows and columns, just like a real table. 
 * The fields in a view are fields from one or more real tables in the database.
 * ( definition taken from w3schools.com )
 */
class fSQLView extends fSQLTable
{
	var $query = null;
	var $columns = null;
	var $entries = null;
	
        /**
	* Closes view by calling father class close and unsets attributes.
	* @param string $query 
        */
	function close()
	{
		parent::close();
		unset($this->query, $this->columns, $this->entries);
	}
	
	function define($query, $columns)
	{
		return false;
	}
	
        /**
	* Sets query.
	* @param string $query 
        */
	function setQuery($query)
	{
		$this->query = $query;
	}

        /**
	* Returns query.
	* @return string 
        */
	function getQuery()
	{
		return $this->query;
	}

        /**
        * Executes query and establishes view (i.e dynamic table) according to
	* query result. View's columns is assigned with result set's columns and
	* entries is assigned with result set's data.
	* @return bool 
        */
	function execute()
	{
		$database =& $this->schema->getDatabase();
		$env =& $database->getEnvironment();
		$rs_id = $env->query($this->query);
		$rs =& $env->get_result_set($rs_id);
		if($rs !== false)
		{
			if($this->definition->getColumns() === false)
				$this->definition->setColumns($rs->columns);
			$this->entries = $rs->data;
			$env->free_result($rs_id);
			return true;
		}
		else
			return false;
	}
}

class fSQLKey
{
	function close()
	{
		return true;
	}
	
	function addEntry($rowid, $values)
	{
		return false;
	}
	
	function create($columns)
	{
		return false;
	}
	
	function deleteEntry($rowid)
	{
		return false;
	}
	
	function drop()
	{
		return $this->close();
	}
	
	/**
	 * Given a row from this key's table extract the key data
	 * for use as a parameter to lookup().
	 * 
	 * @param array $row
	 */
	function extractIndex($row)
	{
		$columns = $this->getColumns();
		if($columns)
		{
			switch(count($columns))
			{
				case 1:
					return $row[$columns[0]];
				case 2:
					return array($row[$columns[0]], $row[$columns[1]]);
				case 3:
					return array($row[$columns[0]], $row[$columns[1]], $row[$columns[2]]);
				default:
					// ugly but it works.  last resort
					return array_intersect_key($row, array_flip($columns));
			}
		}
		else
			return false;
	}
	
	function getColumns() { return false; }
	function getType() { return false; }
	
	function lookup($key)
	{
		return false;
	}
	
	function updateEntry($rowid, $values)
	{
		return $this->deleteEntry($rowid) && $this->addEntry($rowid, $values);
	}
}

?>