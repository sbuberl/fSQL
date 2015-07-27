<?php

class fSQLCursor
{
	var $entries;
	var $current_row_id;
	var $num_rows;
	
	function fSQLCursor(&$entries)
	{
		$this->entries =& $entries;
		$this->num_rows = count($entries);
		$this->first();
	}
	
	function close()
	{
		unset($this->entries, $this->current_row_id, $this->num_rows);	
	}
	
	function currentRowId()
	{
		return $this->current_row_id;
	}
	
	function findRow($row_id)
	{
		$this->first();
		while($this->current_row_id !== false)
		{
			if($this->current_row_id === $row_id)
				return true;
			$this->next();
		}
		return false;
	}

	function first()
	{
		if(reset($this->entries) !== false)
			$this->current_row_id = key($this->entries);
		else
			$this->current_row_id = false;
		return $this->current_row_id;
	}
	
	function getRow()
	{
		return $this->current_row_id !== false ? $this->entries[$this->current_row_id] : null;
	}
	
	function isDone()
	{
		return $this->current_row_id === false;
	}
	
	function last()
	{
		if(end($this->entries) !== false)
			$this->current_row_id = key($this->entries);
		else
			$this->current_row_id = false;
		return $this->current_row_id;
	}
	
	function numRows()
	{
		return $this->num_rows;
	}
	
	function previous()
	{
		if(prev($this->entries) !== false)
			$this->current_row_id = key($this->entries);
		else
			$this->current_row_id = false;
		return $this->current_row_id;
	}
	
	function next()
	{
		if(next($this->entries) !== false)
			$this->current_row_id = key($this->entries);
		else
			$this->current_row_id = false;
		return $this->current_row_id;		
	}
	
	function seek($pos)
	{
		if($pos >=0 && $pos < count($this->entries))
		{
			reset($this->entries);
			for($i = 0; $i < $pos; $i++, next($this->entries)) { }
			$this->current_row_id = key($this->entries);
			return $this->current_row_id;
		}
		else
			return false;
	}
}

class fSQLWriteCursor extends fSQLCursor
{
	var $table;
	
	function fSQLWriteCursor(&$entries, &$table)
	{
		parent::fSQLCursor($entries);
		$this->table =& $table;
	}
	
	function close()
	{
		unset($this->table);  // don't close.  same reference probably exists elsewhere.
		parent::close();
	}
	
	function appendRow($entry, $rowid = false)
	{
		if($rowid !== false)
		{
			$this->entries[$rowid] = $entry;
		}
		else
		{
			$this->entries[] = $entry;
			$aKeys = array_keys($this->entries);
			$rowid = end($aKeys);
		}
		
		$this->num_rows++;
		$keys = $this->table->getKeys();
		if(!empty($keys))
		{
			foreach(array_keys($keys) as $k)
			{
				$key =& $keys[$k];
				$idx = $key->extractIndex($entry);
				$key->addEntry($rowid, $idx);
			}
		}
		
		return $rowid;
	}

	function updateRow($updates)
	{
		$row_id = $this->current_row_id;
		if($row_id !== false)
		{
			foreach($updates as $column => $value)
				$this->entries[$row_id][$column] = $value;
			
			$keys = $this->table->getKeys();
			foreach(array_keys($keys) as $k)
			{
				$key =& $keys[$k];
				$idx = $key->extractIndex($this->entries[$row_id]);
				$key->updateEntry($row_id, $idx);
			}
		}
	}

	function deleteRow()
	{
		$row_id = $this->current_row_id;
		if($this->current_row_id !== false)
		{
			$this->num_rows--;
			unset($this->entries[$row_id]);
			$this->current_row_id = key($this->entries);
			if($this->current_row_id === null) { // key on an empty array is null?
				$this->current_row_id = false;
				$this->entries = array();
			}
			
			$keys = $this->table->getKeys();
			foreach(array_keys($keys) as $k)
			{
				$key =& $keys[$k];
				$key->deleteEntry($row_id);
			}
		}
	}
}

class fSQLResultSet
{
	var $columnNames;  // cached to save speed iterating
	var $columns;
	var $data;
	var $columnsCursor;
	var $dataCursor;
	
	function fSQLResultSet($columns, $data)
	{
		$this->columns = $columns;
		$this->columnNames = array();
		foreach($columns as $column)
			$this->columnNames[] = $column['name'];
		$this->data = $data;
		$this->columnsCursor =& new fSQLCursor($columns);
		$this->dataCursor =& new fSQLCursor($data);
	}
	
	function free()
	{
		unset(
			$this->columnNames,
			$this->columns,
			$this->data,
			$this->columnsCursor,
			$this->dataCursor
		);
		return true;
	}
}

?>