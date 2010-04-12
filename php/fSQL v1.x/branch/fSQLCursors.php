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
	var $newRows = array();
	
	var $updatedRows = array();
	
	var $deletedRows = array();
	
	function close()
	{
		unset($this->newRows, $this->updatedRows, $this->deletedRows);
		parent::close();
	}
	
	function appendRow($entry)
	{
		$this->entries[] = $entry;
		$aKeys = array_keys($this->entries);
		$this->newRows[] = end($aKeys);
		$this->num_rows++;
	}
	
	function getNewRows()
	{
		return array_intersect_key($this->entries, array_flip($this->newRows));
	}

	function updateField($column, $value)
	{
		$row_id = $this->current_row_id;
		if($row_id !== false)
		{
			if(isset($this->newRows[$row_id])) // row add in same transaction
			{
				$this->newRows[$row_id][$column] = $value; 
			}
			else
			{
				if(!isset($this->updatedRows[$row_id]))
					$this->updatedRows[$row_id] = array($column => $value);
				else
					$this->updatedRows[$row_id][$column] = $value;
			}
		
			$this->entries[$row_id][$column] = $value;
		}
	}

	function deleteRow()
	{
		$row_id = $this->current_row_id;
		if($this->current_row_id !== false)
		{
			$this->num_rows--;
			if(isset($this->newRows[$row_id])) // row added in same transaction
				unset($this->newRows[$row_id]);
			else if(!in_array($row_id, $this->deletedRows)) // double check not already in there
				$this->deletedRows[] = $row_id;
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
		return !empty($this->newRows) || !empty($this->updatedRows) || !empty($this->deletedRows);
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