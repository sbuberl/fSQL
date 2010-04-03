<?php
/**
 * Driver module base classes file
 * 
 * This file is describes base classes for drivers module. 
 * @author Kaja Fumei <kaja.fumei@gmail.com>
 * @version 
 */

/**
 * Base driver class. Has no attribute.
 */
class fSQLDriver
{
	function &defineDatabase(&$environment, $name)
	{
		return false;
	}	
}

/**
 * Base database class. This class provides basic properties for any kind of 
 * database.
 */
class fSQLDatabase
{
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
	function fSQLDatabase(&$environment, $name, $path)
	{
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
		return false;
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
        * Creates a schema in this database. This class does not implement 	
	* method, subclasses will do.
	* @return fSQLSchema|bool new schema or false
        */
	function &_createSchema($name)
	{
		$schema = false;
		return $schema;
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
			$this->schemas[$name] =& $this->_createSchema($name);
			if($this->schemas[$name]->create())
				return $this->schemas[$name];
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
		return false;
	}
	
        /**
        * Drops schema. This class does not implement method, subclasses will do.
	* @return bool always true
        */
	function drop()
	{
		return true;
	}

        /**
        * Closes schema as unsetting attributes.
        */
	function close()
	{
		unset($this->name, $this->database);
	}
	
        /**
        * Creates table. This class does not implement method, subclasses will do.
	* @param string $table_name
	* @param string $columns
	* @param bool $temporary
	* @return fSQLTable always null
        */
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = null;
		return $table;
	}
	
        /**
        * Creates view. This class does not implement method, subclasses will do.
	* @param string $view_name
	* @param string $query
	* @param string $columns
	* @return fSQLView always null
        */
	function &createView($view_name, $query, $columns = null)
	{
		$view = null;
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
