<?php

namespace FSQL\Database;

class MemoryKey extends Key
{
    private $name;
    private $type;
    private $columns;
    private $key = [];

    public function __construct($name, $type, array $columns)
    {
        $this->name = $name;
        $this->type = $type;
        $this->columns = $columns;
    }

    public function name() { return $this->name; }
    public function columns() { return $this->columns; }
    public function type() { return $this->type; }
    public function count() { return count($this->key); }

    private function buildKeyIndex($values)
    {
        if(is_array($values) && count($values) > 1)
            return serialize($values);
        else
            return $values;
    }
    
    public function reset()
    {
        $this->key = [];
    }
	
	public function addEntry($rowId, $values)
	{
		$index = $this->buildKeyIndex($values);
		$this->key[$index] = $rowId;
		return true;
	}
	
	public function deleteEntry($rowId)
	{
		$index = array_search($rowId, $this->key);
		if($index !== false && $index !== null)
			unset($this->key[$index]);
		return true;
	}
	
	public function lookup($key)
	{
		$index = $this->buildKeyIndex($key);
		return isset($this->key[$index]) ? $this->key[$index] : false;
	}
}