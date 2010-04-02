<?php

class fSQLMemoryDriver extends fSQLDriver
{
	function &defineDatabase(&$environment, $name)
	{
		$db =& new fSQLMemoryDatabase($environment, $name);
		return $db;
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
		return $this->defineSchema('public') !== false;
	}
	
	function &_createSchema($name)
	{
		$schema =& new fSQLMemorySchema($this, $name);
		return $schema;
	}
}

class fSQLMemorySchema extends fSQLSchema
{
	var $tables = array();

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
		$table =& new fSQLMemoryTable($table_name, $this);
		$this->tables[$table_name] =& $table;
		//$table =& $this->tables[$table_name];
		$table->create($columns);
		return $table;
	}
	
	function &createView($view_name, $query, $columns = null)
	{
		$table =& new fSQLMemoryView($view_name, $this);
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

class fSQLMemoryTableDef extends fSQLTableDef
{
	var $columns = null;
	
	function close()
	{
		$this->columns = null;
	}
	
	function drop()
	{
		$this->close();
		return true;
	}

	function getColumns()
	{
		return $this->columns;
	}
	
	function setColumns($columns)
	{
		$this->columns = $columns;
	}
	
	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
}

/**
 * Class for temporary and in-memory tables.
 */
class fSQLMemoryTable extends fSQLTable
{
	var $rcursor = null;
	var $wcursor = null;
	var $entries = null;

	function fSQLMemoryTable($name, &$schema)
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

class fSQLMemoryView extends fSQLView
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

?>