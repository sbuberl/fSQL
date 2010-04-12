<?php

class fSQLMemoryDriver extends fSQLDriver
{
	function &defineDatabase(&$environment, $name, $args)
	{
		$db =& new fSQLMemoryDatabase($this, $environment, $name);
		return $db;
	}
	
	function isAbstract()
	{
		return false;
	}
	
	function &newSchemaObj(&$db, $name)
	{
		$schema =& new fSQLMemorySchema($db, $name);
		return $schema;
	}
	
	function &newTableObj(&$schema, $name)
	{
		$table =& new fSQLMemoryTable($schema, $name);
		return $table;
	}
	
	function &newTableDefObj(&$schema, $table_name)
	{
		$def =& new fSQLMemoryTableDef($schema, $table_name);
		return $def;
	}
	
	function &newViewObj(&$schema, $name)
	{
		$view =& new fSQLMemoryView($schema, $name);
		return $view;
	}
}

class fSQLMemoryDatabase extends fSQLDatabase
{
	function fSQLMemoryDatabase(&$driver, &$environment, $name)
	{
		parent::fSQLDatabase($driver, $environment, $name, FSQL_MEMORY_DB_PATH);
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
	
	function &createKey($name, $type, $columns, &$table)
	{
		$key =& $table->createKey($name, $type, $columns);
		return $key;
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table =& parent::createTable($table_name, $columns, $temporary);
		if($table !== false)
			$this->tables[$table_name] =& $table;
		return $table;
	}
	
	function &createView($view_name, $query, $columns = null)
	{
		$view =& parent::createView($view_name, $query, $columns);
		if($view !== false)
			$this->tables[$view_name] =& $view;
		return $view;
	}
	
	function &getTable($table_name)
	{
		$rel = false;
		if(isset($this->tables[$table_name]))
		{
			$rel =& $this->tables[$table_name];
			if(fsql_is_a($rel, 'fSQLView')) {
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
			unset($this->tables[$table_name]);
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
		unset($this->columns);
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
	function readLock() { return true; }
	function writeLock() { return true; }
	function unlock() { return true; }
}

/**
 * Class for temporary and in-memory tables.
 */
class fSQLMemoryTable extends fSQLTable
{
	var $rcursor = null;
	var $wcursor = null;
	var $entries = null;
	var $keys = array();

	function fSQLMemoryTable(&$schema, $name)
	{
		parent::fSQLTable($schema, $name);
		$db =& $schema->getDatabase();
		$driver =& $db->getDriver();
		$this->definition =& $driver->newTableDefObj($schema, $name);
	}

	function close()
	{
		unset($this->rcursor);
		unset($this->wcursor);
		unset($this->entries);
		unset($this->keys);
	}
	
	function create($columnDefs)
	{
		$this->definition->setColumns($columnDefs);
		$this->entries = array();
	}
	
	function &createKey($name, $type, $columns)
	{
		$key =& new fSQLMemoryKey($type);
		$this->keys[$name] =& $key;
		$key->create($columns);
		return $key;
	}

	function temporary() {
		return true;
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
			$this->wcursor =& new fSQLMemoryWriteCursor($this->entries, $this);
		
		return $this->wcursor;
	}
	
	function getEntries()
	{
		return $this->entries;
	}
	
	function getKeys()
	{
		return $this->keys;
	}
	
	function commit()
	{
		return true;
	}
	
	function rollback()
	{
		return false;
	}

	// Free up all data
	function drop()
	{
		$this->definition->drop();
		$this->close();
		return true;
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

class fSQLMemoryWriteCursor extends fSQLWriteCursor
{	
	var $table;
	
	function fSQLMemoryWriteCursor(&$entries, &$table)
	{
		parent::fSQLCursor($entries);
		$this->table =& $table;
	}
	
	function appendRow($entry)
	{
		$this->entries[] = $entry;
		$this->num_rows++;
		$aKeys = array_keys($this->entries);
		$rowId = end($aKeys);
		$keys = $this->table->getKeys();
		foreach(array_keys($keys) as $k)
		{
			$key =& $keys[$k];
			$idx = $key->extractIndex($entry);
			$key->addEntry($rowId, $idx);
		}
	}

	function updateField($column, $value)
	{
		$row_id = $this->current_row_id;
		if($row_id !== false)
		{
			$this->entries[$row_id][$column] = $value;
		}
	}

	function deleteRow()
	{
		$row_id = $this->current_row_id;
		if($this->current_row_id !== false)
		{
			unset($this->entries[$row_id]);
			$this->current_row_id = key($this->entries);
			if($this->current_row_id === null) { // key on an empty array is null?
				$this->current_row_id = false;
				$this->entries = array();
			}
		}
	}
	
	function isUncommitted()
	{
		return false;
	}
}

class fSQLMemoryKey extends fSQLKey
{
	var $key = array();
	var $type;
	var $columns = null;
	
	function fSQLMemoryKey($type)
	{
		$this->type = $type;
	}
	
	function _buildKeyIndex($values)
	{
		if(count($values) > 1)
			return serialize($values);
		else
			return $values;
	}
	
	function addEntry($rowid, $values)
	{
		$idx = $this->_buildKeyIndex($values);
		$this->key[$idx] = $rowid;
		return true;
	}
	
	function close()
	{
		unset($this->type, $this->columns);
		return true;
	}
	
	function create($columns)
	{
		$this->columns = $columns;
		return true;
	}
	
	function deleteEntry($rowid, $values)
	{
		$idx = $this->_buildKeyIndex($values);
		if(isset($this->key[$idx]))
			unset($this->key[$idx]);
		return true;
	}
	
	function getColumns() { return $this->columns; }
	function getType() { return $this->type; }
	
	function lookup($key)
	{
		$idx = $this->_buildKeyIndex($key);
		return isset($this->key[$idx]) ? $this->key[$idx] : false;
	}
}

?>