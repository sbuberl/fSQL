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
	var $uncommitted = false;

	function fSQLWriteCursor(&$entries)
	{
		parent::fSQLCursor($entries);
	}
	
	function appendRow($entry)
	{
		array_push($this->entries, $entry);
		$this->uncommitted = true;
	}

	function updateField($column, $value)
	{
		if($this->current_row_id !== false)
		{
			$this->entries[$this->current_row_id][$column] = $value;
			$this->uncommitted = true;
		}
	}

	function deleteRow()
	{
		if($this->current_row_id !== false)
		{
			$this->num_rows--;
			unset($this->entries[$this->current_row_id]);
			$this->current_row_id = key($this->entries);
			if($this->current_row_id === null) { // key on an empty array is null?
				$this->current_row_id = false;
				$this->entries = array();
			}
			$this->uncommitted = true;
		}
	}
	
	function isUncommitted()
	{
		return $this->uncommitted;
	}
}

class fSQLResultSet
{
	var $columns;
	var $data;
	var $columnsCursor;
	var $dataCursor;
	
	function fSQLResultSet($columns, $data)
	{
		$this->columns = array();
		foreach($columns as $column)
			$this->columns[$column['name']] = $column;
		$this->data = $data;
		$this->columnsCursor =& new fSQLCursor($columns);
		$this->dataCursor =& new fSQLCursor($data);
	}
	
	function free()
	{
		unset($this->columns);
		unset($this->data);
		unset($this->columnsCursor);
		unset($this->dataCursor);
		return true;
	}
}

?>