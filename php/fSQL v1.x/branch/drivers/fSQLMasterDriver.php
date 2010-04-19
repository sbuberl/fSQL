<?php

class fSQLMasterDriver extends fSQLMemoryDriver
{
	function &defineDatabase(&$environment, $name, $args)
	{
		$db =& new fSQLMasterDatabase($this, $environment, $name);
		return $db;
	}
	
	function &newSchemaObj(&$db, $name)
	{
		$schema =& new fSQLMasterSchema($db, $name);
		return $schema;
	}
}

class fSQLMasterDatabase extends fSQLMemoryDatabase
{
	function create()
	{
		return $this->defineSchema('master') !== false;
	}
}

class fSQLMasterSchema extends fSQLMemorySchema
{	
	function create()
	{
		$databasesTable =& $this->createTable('databases', array(
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'path' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null)
			)
		);
		
		$schemasTable =& $this->createTable('schemas', array(
				'database' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'path' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null)
			)
		);
		
		$tablesTable =& $this->createTable('tables', array(
				'database' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'schema' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'type' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null)
			)
		);
		
		$columnsTable =& $this->createTable('columns', array(
				'database' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'schema' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'table' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'name' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'p', 'restraint' => null),
				'type' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null),
				'default' => array('type' => FSQL_TYPE_STRING, 'default' => null, 'null' => true, 'identity' => null, 'key' => 'n', 'restraint' => null),
				'key' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null),
				'nullable' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null),
				'identity' => array('type' => FSQL_TYPE_STRING, 'default' => '', 'null' => false, 'identity' => null, 'key' => 'n', 'restraint' => null)
			)
		);
		
		return true;
	}
	
	function addDatabase(&$database)
	{
		$dbTable =& $this->getTable('databases');
		$dbCursor =& $dbTable->getWriteCursor();
		$dbCursor->appendRow(array($database->getName(), $database->getPath()));
	}
	
	function addSchema(&$schema)
	{
		$database =& $schema->getDatabase();
		$schemaTable =& $this->getTable('schemas');
		$schemaCursor =& $schemaTable->getWriteCursor();
		$schemaCursor->appendRow(array($database->getName(), $schema->getName(), $schema->getPath()));
		
		foreach($schema->listTables() as $table_name)
		{
			$table =& $schema->getTable($table_name);
			$this->addTable($table);
		}
	}
	
	function addTable(&$table)
	{
		$schema =& $table->getSchema();
		$database =& $schema->getDatabase();
		$tablesTable =& $this->getTable('tables');
		$tablesCursor =& $tablesTable->getWriteCursor();
		if(fsql_is_a($table, 'fSQLView'))
			$type = 'VIEW';
		else if($table->temporary()) {
			$type = 'LOCAL TEMPORARY';
		} else {
			$type = 'BASE TABLE';
		}
		$tablesCursor->appendRow(array($database->getName(), $schema->getName(), $table->getName(), $type));
		
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
		
		$tableDef =& $table->getDefinition();
		
		$columnsTable =& $this->getTable('columns');
		$columnsCursor =& $columnsTable->getWriteCursor();
		
		foreach($tableDef->getColumns() as $col_name => $columnDef)
		{
			$type = $environment->_typecode_to_name($columnDef['type']);
			$nullable = $columnDef['type'] ? 'YES' : 'NO';
			$identity = $columnDef['identity'] ? 'YES' : 'NO';
			switch($columnDef['key'])
			{
				case 'p': $key = 'PRIMARY';	break;
				case 'u': $key = 'UNIQUE';	break;
				case 'k': $key = 'KEY';		break;
				default:  $key = '';  break;
			}
			$default = $columnDef['default'] !== null ? (string) $columnDef['default'] : null;
			$columnsCursor->appendRow(array($db_name, $schema_name, $table_name, $col_name, $type, $default, $key, $nullable, $identity));
		}
	}
	
	function removeDatabase(&$database)
	{	
		$db_name = $database->getName();
		
		$databasesTables =& $this->getTable('databases');
		$databasesCursor =& $databasesTables->getWriteCursor();
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
		$database =& $schema->getDatabase();
		$db_name = $database->getName();
		$schema_name = $schema->getName();
		
		$schemasTable =& $this->getTable('schemas');
		$schemasCursor =& $schemasTable->getWriteCursor();
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
		$database =& $schema->getDatabase();
		$db_name = $database->getName();
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
		$database =& $schema->getDatabase();
		$db_name = $database->getName();
		$schema_name = $schema->getName();
		$table_name = $table->getName();
		
		$columnsTable =& $this->getTable('columns');
		$columnsCursor =& $columnsTable->getWriteCursor();
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