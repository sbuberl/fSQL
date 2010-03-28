<?php

class fSQLDeleteQuery extends fSQLDMLQuery
{
	var $tableNamePieces = null;
	
	var $where = null;
	
	function fSQLDeleteQuery(&$environment, $tableNamePieces, $where)
	{
		parent::fSQLDMLQuery($environment);
		$this->tableNamePieces = $tableNamePieces;
		$this->where = $where;
	}
	
	function execute()
	{
		$this->affected  = 0;
		$table =& $this->environment->_find_table($this->tableNamePieces);
		if($table === false)
			return false;
		else if($table->isReadLocked())
			return $this->environment->_error_table_read_lock($this->tableNamePieces);
		
		$cursor =& $table->getWriteCursor();

		if($cursor->isDone())
			return true;
		
		if($this->where !== null)
		{	
			$code = <<<EOC
			while(!\$cursor->isDone()) {
				\$entry = \$cursor->getRow();
				if({$this->where})
				{
					\$cursor->deleteRow();
					\$this->affected++;
				}
				else
					\$cursor->next();
			}
EOC;
			eval($code);
		} else {
			while(!$cursor->isDone()) {
				$cursor->deleteRow();
				$this->affected++;
			}
		}
			
		if($this->affected)
		{
			if($this->environment->auto)
				$table->commit();
			else if(!in_array($table, $this->environment->updatedTables))
				$this->environment->updatedTables[] =& $table;
		}

		return true;
	}	
}

?>