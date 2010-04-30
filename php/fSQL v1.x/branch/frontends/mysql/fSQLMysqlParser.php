<?php

class fSQLParserMySQL extends fSQLParser
{	
	function loadMySQLQueryClass($command)
	{
		return $this->loadExtensionQueryClass($command, 'mysql');
	}
	
	function getTypeParseRegex()
	{
		return '(?:TINY|MEDIUM|LONG)(?:TEXT|BLOB)|(?:TINY|MEDIUM)INT|DATETIME|YEAR|SET'.parent::getTypeParseRegex();
	}
	
	function parseQuery($command, $query)
	{
		switch($command)
		{
			case 'backup':
				return $this->parseBackup($query);
			case 'desc':
			case 'describe':
				return $this->parseDescribe($query);
			case 'lock':
				return $this->parseLock($query);
			case 'rename':
				return $this->parseRename($query);
			case 'restore':
				return $this->parseRestore($query);
			case 'show':
				return $this->parseShow($query);
			case 'unlock':
				return $this->parseUnlock($query);
			case 'use':
				return $this->parseUse($query);
			default:
				return parent::parseQuery($query);
		}
	}
	
	function parseBackup($query)
	{
		if(preg_match("/\ABACKUP\s+TABLE\s+(.*?)\s+TO\s+'(.*?)'\s*[;]?\s*\Z/is", $query, $matches))
		{		
			$tables = explode(',', $matches[1]);
			$table_names = array();
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $table_name_matches)) {
					$table_names[] = $this->_parse_table_name($table_name_matches[1]);
				} else {
					return $this->_set_error('Parse error in table listing');
				}
			}
			
			$this->loadMySQLQueryClass('backup');
			return new fSQLBackupQuery($this->environment, $matches[2], $table_names);
		} else {
			return $this->_set_error('Invalid BACKUP Query');
		}
	}
	
	function parseDescribe($query)
	{
		if(preg_match('/\ADESC(?:RIBE)?\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s*[;]?\s*\Z/is', $query, $matches)) {
			$table_name_pieces = $this->environment->_parse_table_name($matches[1]);
			$this->loadMySQLQueryClass('showColumns');
			return $table_name_pieces !== false ? new fSQLShowColumnsQuery($this->environment, $table_name_pieces, false) : false;
		} else {
			return $this->environment->_set_error('Invalid DESCRIBE query');
		}
	}
	
	function parseLock($query)
	{
		if(preg_match('/\ALOCK\s+TABLES\s+(.+?)\s*[;]?\s*\Z/is', $query, $matches)) {
			preg_match_all('/+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+((?:READ(?:\s+LOCAL)?)|((?:LOW\s+PRIORITY\s+)?WRITE))/is', $matches[1], $rules);
			$numRules = count($rules[0]);
			$read_table_names = array();
			$write_table_names = array();
			for($r = 0; $r < $numRules; $r++) {
				$table_name_pieces = $this->environment->_parse_table_name($rules[1][$r]);
				$table =& $this->environment->_find_table($table_name_pieces);
				if($table === false)
					return false;
				
				if(!strncasecmp($rules[4][$r], 'READ', 4)) {
					$read_table_names[] = $table_name_pieces;
				}
				else {  /* WRITE */
					$write_table_names[] = $table_name_pieces;
				}
			}
			
			$this->loadMySQLQueryClass('lock');
			return new fSQLLockQuery($this->environment, $read_table_names, $write_table_names);
		} else {
			return $this->environment->_set_error('Invalid LOCK query');
		}
	}
	
	function parseRename($query)
	{
		if(preg_match('/\ARENAME\s+TABLE\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$tables = explode(',', $matches[1]);
			$renamed = array();
			foreach($tables as $table) {
				list($old, $new) = preg_split('/\s+TO\s+/i', trim($table));
				
				$rename = array();
				$rename['old'] = $this->environment->_parse_table_name($old);
				$rename['new'] = $this->environment->_parse_table_name($new);
				$renamed[] = $rename;
			}
			
			$this->loadMySQLQueryClass('rename');
			return new fSQLRenameQuery($this->environment, $renamed);
		} else {
			return $this->environment->_set_error('Invalid RENAME query');
		}
	}
	
	function parseRestore($query)
	{
		if(preg_match("/\ARESTORE\s+TABLE\s+(.*?)\s+FROM\s+'(.*?)'\s*[;]?\s*\Z/is", $query, $matches))
		{
			$tables = explode(',', $matches[1]);
			$table_names = array();
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $table_name_matches)) {
					$table_names[] = $this->_parse_table_name($table_name_matches[1]);
				} else {
					return $this->_set_error('Parse error in table listing');
				}
			}
			
			$this->loadMySQLQueryClass('restore');
			return new fSQLRestoreQuery($this->environment, $matches[2], $table_names);
		} else {
			return $this->_set_error('Invalid RESTORE Query');
		}
	}
	
	function parseShow($query)
	{
		if(preg_match('/\ASHOW\s+(FULL\s+)?TABLES(?:\s+(?:FROM|IN)\s+(`?(?:[^\W\d]\w*`?\.`?)?[^\W\d]\w*`?))?(?:\s+ORDER\s+BY\s+(.*?))?\s*[;]?\s*\Z/is', $query, $matches))
		{	
			$full = !empty($matches[1]);
			$schema_name = isset($matches[2]) ? $matches[2] : null;
			$order_clause = isset($matches[3]) ? $matches[3] : null;
			$schema_name_pieces = null;
			
			if(isset($matches[2]))
			{
				$schema_name_pieces = $this->environment->_parse_schema_name($schema_name);
				if($schema_name_pieces !== false) {
					$schema =& $this->environment->_find_schema($schema_name_pieces[0], $schema_name_pieces[1]);
				}
				else
					return false;
			} else {
				$schema =& $this->environment->currentSchema;
			}
			
			if($schema === false)
				return false;
			
			$columns = array(
						array('name' => 'Tables_in_'.$database->getName().'_'.$schema->getName(),'type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null)
					);
		
			if($full)
				$columns[] = array('name'=>'Table_type','type'=>FSQL_TYPE_STRING,'default'=>'','null'=>false,'auto'=>'false','key'=>'n','restraint'=>null);
			
			$orderby = null;
			
			if($order_clause !== null) {
				$ORDERBY = explode(',', $order_clause);
				if(!empty($ORDERBY))
				{
					$tosort = array();
					
					foreach($ORDERBY as $order_item)
					{
						if(preg_match('/([^\W\d]\w*)(?:\s+(ASC|DESC))?/is', $order_item, $additional)) {
							$index = array_search($additional[1], $columns);
							if(empty($additional[2])) { $additional[2] = 'ASC'; }
							$tosort[] = array('key' => $index, 'ascend' => !strcasecmp('ASC', $additional[2]));
						}
					}
					
					$orderby = new fSQLOrderByClause($tosort);
				}
			}
			
			$this->loadMySQLQueryClass('showTables');
			return new fSQLShowTablesQuery($this->environment, $schema_name_pieces, $full, $orderby);
		} else if(preg_match('/\ASHOW\s+DATABASES\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->loadMySQLQueryClass('showDatabases');
			return new fSQLShowDatabasesQuery($this->environment);
		} else if(preg_match('/\ASHOW\s+(FULL\s+)?COLUMNS\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?(?:\s+(?:FROM|IN)\s+`?(?:([^\W\d]\w*)`?\.`?)?([^\W\d]\w*)`?)?\s*[;]?\s*\Z/is', $query, $matches)) {
			$db_name = isset($matches[3]) ? $matches[3] : null;
			$schema_name = isset($matches[4]) ? $matches[4] : null;
			$this->loadMySQLQueryClass('showColumns');
			return new fSQLShowColumnsQuery($this->environment, array($db_name, $schema_name, $matches[2]), !empty($matches[1]));
		} else {
			return $this->environment->_set_error('Invalid SHOW query');
		}
	}
	
	function parseUnlock($query)
	{
		if(preg_match('/\AUNLOCK\s+TABLES\s*[;]?\s*\Z/is', $query)) {
			$this->loadMySQLQueryClass('unlock');
			return new fSQLUnlockQuery($this->environment);
		} else {
			return $this->environment->_set_error('Invalid UNLOCK query');
		}
	}
	
	function parseUse($query)
	{
		if(preg_match('/\AUSE\s+`?([^\W\d]\w*)`?\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->loadMySQLQueryClass('use');
			return new fSQLUseQuery($this->environment, $matches[1]);
		} else {
			return $this->environment->_set_error('Invalid USE query');
		}
	}
}

?>