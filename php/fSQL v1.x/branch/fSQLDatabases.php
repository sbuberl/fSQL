<?php

class fSQLDatabase
{
	var $name = null;
	var $path = null;
	var $schemas = array();
	var $environment = null;

	function fSQLDatabase(&$environment, $name, $path)
	{
		$this->environment =& $environment;
		$this->name = $name;
		$this->path = $path;
	}
	
	function create()
	{
		$path = create_directory($this->path, 'database', $this->environment);
		if($path !== false) {
			$this->path = $path;
			$this->defineSchema('public');
			$this->environment->_get_master_schema()->addDatabase($this);
			return true;
		}
		else
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
		$schema =& new fSQLStandardSchema($this, $name);
		return $schema;
	}
	
	function defineSchema($name)
	{
		if(!isset($this->schemas[$name]))
		{
			$this->schemas[$name] =& $this->_createSchema($name);
			return $this->schemas[$name]->create();
		}
		else
			return false;
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
		$this->environment->_get_master_schema()->removeDatabase($this);
		foreach(array_keys($this->schemas) as $schema_name)
			$this->schemas[$schema_name]->drop();
		$this->close();
		return true;
	}
}

class fSQLMemoryDatabase extends fSQLDatabase
{
	function fSQLMemoryDatabase(&$environment, $name)
	{
		parent::fSQLDatabase($environment, $name, FSQL_MEMORY_DB_PATH);
	}
	
	function create()
	{
		$this->defineSchema('public');
		$this->environment->_get_master_schema()->addDatabase($this);
		return true;
	}
	
	function defineSchema($name)
	{
		if(!isset($this->schemas[$name]))
		{
			$this->schemas[$name] =& new fSQLMemorySchema($this, $name);
			return $this->schemas[$name]->create();
		}
		else
			return false;
	}
	
	function &_createSchema($name)
	{
		$schema =& new fSQLMemorySchema($this, $name);
		return $schema;
	}
}

class fSQLMasterDatabase extends fSQLMemoryDatabase
{
	function fSQLMemoryDatabase(&$environment, $name)
	{
		parent::fSQLMemoryDatabase($environment, $name);
	}
	
	function create()
	{
		return $this->defineSchema('master');
	}
	
	function &_createSchema($name)
	{
		$schema =& new fSQLMasterSchema($this, $name);
		return $schema;
	}
	
	function defineSchema($name)
	{
		if(!isset($this->schemas[$name]))
		{
			$this->schemas[$name] =& new fSQLMasterSchema($this, $name);
			return $this->schemas[$name]->create();
		}
		else
			return false;
	}
}

?>