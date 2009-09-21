<?php

define('FSQL_VERSION', '1.4.0');
define('FSQL_MEMORY_DB_PATH', ':memory:');

define('FSQL_ASSOC',1,TRUE);
define('FSQL_NUM',  2,TRUE);
define('FSQL_BOTH', 3,TRUE);

define('FSQL_TRUE', 3, TRUE);
define('FSQL_FALSE', 0,TRUE);
define('FSQL_NULL', 1,TRUE);
define('FSQL_UNKNOWN', 1,TRUE);

define('FSQL_JOIN_INNER',0,TRUE);
define('FSQL_JOIN_LEFT',1,TRUE);
define('FSQL_JOIN_RIGHT',2,TRUE);
define('FSQL_JOIN_FULL',3,TRUE);

define('FSQL_WHERE_NORMAL',2,TRUE);
define('FSQL_WHERE_NORMAL_AGG',3,TRUE);
define('FSQL_WHERE_ON',4,TRUE);
define('FSQL_WHERE_HAVING',8,TRUE);
define('FSQL_WHERE_HAVING_AGG',9,TRUE);

define('FSQL_TYPE_DATE','d',TRUE);
define('FSQL_TYPE_DATETIME','dt',TRUE);
define('FSQL_TYPE_ENUM','e',TRUE);
define('FSQL_TYPE_FLOAT','f',TRUE);
define('FSQL_TYPE_INTEGER','i',TRUE);
define('FSQL_TYPE_NUMERIC','n',TRUE);
define('FSQL_TYPE_STRING','s',TRUE);
define('FSQL_TYPE_TIME','t',TRUE);
define('FSQL_TYPE_TIMESTAMP','ts',TRUE);
define('FSQL_TYPE_YEAR','y',TRUE);

define('FSQL_FUNC_NORMAL', 1, TRUE);
define('FSQL_FUNC_AGGREGATE', 2, TRUE);
define('FSQL_FUNC_ENV', 3, TRUE);

define('FSQL_FORMAT_DATETIME', '%Y-%m-%d %H:%M:%S');
define('FSQL_FORMAT_DATE', '%Y-%m-%d');
define('FSQL_FORMAT_TIME', '%H:%M:%S');

define('FSQL_EXTENSION', '.cgi',TRUE);

if(!defined('FSQL_INCLUDE_PATH')) {
	define('FSQL_INCLUDE_PATH', dirname(__FILE__));
}
require FSQL_INCLUDE_PATH.'/fSQLCursors.php';
require FSQL_INCLUDE_PATH.'/fSQLDatabases.php';
require FSQL_INCLUDE_PATH.'/fSQLFunctions.php';
require FSQL_INCLUDE_PATH.'/fSQLSchemas.php';
require FSQL_INCLUDE_PATH.'/fSQLTables.php';
require FSQL_INCLUDE_PATH.'/fSQLUtilities.php';

class fSQLEnvironment
{
	var $updatedTables = array();
	var $lockedTables = array();
	var $databases = array();
	var $currentDB = null;
	var $currentSchema = null;
	var $error_msg = null;
	var $query_count = 0;
	var $join_lambdas = array();
	var $affected = 0;
	var $insert_id = 0;
	var $auto = 1;
	var $registered_functions = array();
	
	function fSQLEnvironment()
	{
		list($usec, $sec) = explode(' ', microtime());
		srand((float) $sec + ((float) $usec * 100000));
		
		$this->databases['FSQL'] =& new fSQLMasterDatabase($this, 'FSQL');
		$this->databases['FSQL']->create();
	}
	
	function define_db($name, $path)
	{
		$this->error_msg = null;
		
		if($path !== FSQL_MEMORY_DB_PATH)
			$db =& new fSQLDatabase($this, $name, $path);
		else
			$db =& new fSQLMemoryDatabase($this, $name); 
		
		if($db->create()) {
			$this->databases[$name] =& $db;
			return true;
		}
		else
			return false;
	}
	
	function define_schema($db_name, $schema_name)
	{
		$this->error_msg = null;
		
		$db =& $this->_get_database($db_name);
		if($db !== false) {
			return $db->defineSchema($schema_name);
		}
		else
			return false;
	}
	
	function select_db($name)
	{
		if(isset($this->databases[$name])) {
			$this->currentDB =& $this->databases[$name];
			$this->currentSchema =& $this->currentDB->getSchema('public');
			return true;
		} else {
			return $this->_set_error("No database called {$name} found");
		}
	}
	
	function select_schema($db_name, $schema_name)
	{
		if(isset($this->databases[$db_name])) {
			$this->currentDB =& $this->databases[$db_name];
			$schema =& $this->currentDB->getSchema($schema_name);
			if($schema !== false) {
				$this->currentSchema =& $schema;
				return true;
			}
			else
				return false;
		} else {
			return $this->_set_error("No database called {$name} found");
		}
	}
	
	function close()
	{
		$this->_unlock_tables();
		
		foreach (array_keys($this->databases) as $db_name ) {
			$this->databases[$db_name]->close();
		}
		
		$this->updatedTables = array();
		$this->join_lambdas = array();
		$this->databases = array();
		$this->currentDB = null;
		$this->error_msg = null;
	}
	
	function error()
	{
		return $this->error_msg;
	}

	function register_function($sqlName, $phpName)
	{
		$this->registered_functions[$sqlName] = $phpName;
		return true;
	}
	
	function _set_error($error)
	{
		$this->error_msg = $error."\r\n";
		return false;
	}
	
	function _error_schema_not_exist($db_name, $schema_name)
	{
		return $this->_set_error("Schema {$db_name}.{$schema_name} does not exist"); 
	}
	
	function _error_table_not_exists($table_name_pieces)
	{
		return $this->_set_error("Table {$table_name_pieces[0]}.{$table_name_pieces[1]}.{$table_name_pieces[2]} does not exist"); 
	}

	function _error_table_read_lock($table_name)
	{
		return $this->_set_error("Table {$table_name_pieces[0]}.{$table_name_pieces[1]}.{$table_name_pieces[2]} is locked for reading only"); 
	}
	
	function escape_string($string)
	{
		return str_replace(array('\\', '\0', '\n', '\r', '\t', '\''), array('\\\\', '\\0', '\\n', '\\', '\\t', '\\\''), $string);
	}
	
	function affected_rows()
	{
		return $this->affected;
	}

	function insert_id()
	{
		return $this->insert_id;
	}
	
	function list_dbs()
	{
		$databases = array();
		foreach($this->databases as $db_name => $db)
		{
			$databases[$db_name] = $db->getPath();
		}
		return $databases;
	}
	
	function query_count()
	{
		return $this->query_count;
	}
	
	function &_get_master_schema()
	{
		return $this->databases['FSQL']->getSchema('master');
	}
	
	function &_get_database($db_name)
	{
		$db = false;
		
		if(!$db_name)
		{
			if($this->currentDB !== null)
				$db =& $this->currentDB;
			else
				$this->_set_error('No database specified');
		}
		else
		{
			if(isset($this->databases[$db_name]))
				$db =& $this->databases[$db_name];
			else
				$this->_set_error("Database $db_name not found"); 
		}
		
		return $db;
	}
	
	function &_find_schema($db_name, $schema_name)
	{
		$schema = false;
		
		$db =& $this->_get_database($db_name);
		if($db !== false)
		{
			if(!$schema_name)
			{
				if($this->currentSchema !== null)
					$schema =& $this->currentSchema;
				else
					$this->_set_error('No schema selected');
			}
			else
			{
				$schema =& $db->getSchema($schema_name);
				if($schema === false)
					$this->_error_schema_not_exist($db->getName(), $schema_name);
			}
		}
		
		return $schema;
	}
	
	function &_find_table($name_pieces)
	{
		$table = false;
		
		if($name_pieces !== false)
		{
			list($db_name, $schema_name, $table_name) = $name_pieces;
			
			$schema =& $this->_find_schema($db_name, $schema_name);
			if($schema)
			{
				$table =& $schema->getTable($table_name);
			}
		}
		
		return $table;
	}
	
	function _unlock_tables()
	{
		foreach (array_keys($this->lockedTables) as $index )
			$this->lockedTables[$index]->unlock();
		$this->lockedTables = array();
	}

	function _begin()
	{
		$this->auto = 0;
		$this->_unlock_tables();
		$this->_commit();
	}
	
	function _commit()
	{
		$this->auto = 1;
		foreach (array_keys($this->updatedTables) as $index ) {
			$this->updatedTables[$index]->commit();
		}
		$this->updatedTables = array();
	}
	
	function _rollback()
	{
		$this->auto = 1;
		foreach (array_keys($this->updatedTables) as $index ) {
			$this->updatedTables[$index]->rollback();
		}
		$this->updatedTables = array();
	}
	
	function query($query)
	{
		$query = trim($query);
		preg_match("/\A[A-Z]+/i", $query, $function);
		$function = strtoupper($function[0]);
		$this->query_count++;
		$this->error_msg = NULL;
		switch($function) {
			case 'CREATE':		return $this->_query_create($query);
			case 'SELECT':		return $this->_query_select($query);
			//case 'SEARCH':		return $this->_query_search($query);
			case 'INSERT':
			case 'REPLACE':	return $this->_query_insert($query);
			case 'UPDATE':		return $this->_query_update($query);
			case 'ALTER':		return $this->_query_alter($query);
			case 'DELETE':		return $this->_query_delete($query);
			case 'BEGIN':		return $this->_query_begin($query);
			case 'START':		return $this->_query_start($query);
			case 'COMMIT':		return $this->_query_commit($query);
			case 'ROLLBACK':	return $this->_query_rollback($query);
			case 'RENAME':	return $this->_query_rename($query);
			case 'TRUNCATE':	return $this->_query_truncate($query);
			case 'DROP':		return $this->_query_drop($query);
			case 'BACKUP':		return $this->_query_backup($query);
			case 'RESTORE':	return $this->_query_restore($query);
			case 'USE':		return $this->_query_use($query);
			case 'DESC':
			case 'DESCRIBE':	return $this->_query_describe($query);
			case 'SHOW':		return $this->_query_show($query);
			case 'LOCK':		return $this->_query_lock($query);
			case 'UNLOCK':		return $this->_query_unlock($query);
			//case 'MERGE':		return $this->_query_merge($query);
			//case 'IF':			return $this->_query_ifelse($query);
			default:			return $this->_set_error('Invalid Query');
		}
	}
	
	function _query_begin($query)
	{
		if(preg_match('/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is', $query, $matches)) {			
			$this->_begin();
			return true;
		} else {
			return $this->_set_error('Invalid Query');
		}
	}
	
	function _query_start($query)
	{
		if(preg_match('/\ASTART\s+TRANSACTION\s*[;]?\Z/is', $query, $matches)) {			
			$this->_begin();
			return true;
		} else {
			return $this->_set_error('Invalid Query');
		}
	}
	
	function _query_commit($query)
	{
		if(preg_match('/\ACOMMIT\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->_commit();
			return true;
		} else {
			return $this->_set_error('Invalid Query');
		}
	}
	
	function _query_rollback($query)
	{
		if(preg_match('/\AROLLBACK\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->_rollback();
			return true;
		} else {
			return $this->_set_error('Invalid Query');
		}
	}
	
	function _query_create($query)
	{
		if(preg_match('/\ACREATE(?:\s+(TEMPORARY))?\s+TABLE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s*\((.+)\)|\s+LIKE\s+((?:[^\W\d]\w*\.){0,2}[^\W\d]\w*))/is', $query, $matches)) {
			
			list(, $temporary, $ifnotexists, $table_name, $column_list) = $matches;

			$table_name_pieces = $this->_parse_table_name($table_name);
			$table =& $this->_find_table($table_name_pieces);
			$schema = $table->getSchema();
			if($table === false) {
				return false;
			} else if($table->exists()) {
				if(empty($ifnotexists)) {
					return $this->_set_error("Table {$table_name} already exists");
				} else {
					return true;
				}
			}
			
			$temporary = !empty($temporary);			

			if(!isset($matches[5])) {
				//preg_match_all("/(?:(KEY|PRIMARY KEY|UNIQUE) (?:([^\W\d]\w*)\s*)?\((.+?)\))|(?:`?([^\W\d]\w*?)`?(?:\s+((?:TINY|MEDIUM|BIG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)(\s+UNSIGNED)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);
				preg_match_all('/(?:(?:CONSTRAINT\s+(?:[^\W\d]\w*\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+([^\W\d]\w*))?\s*\((.+?)\))|(?:`?([^\W\d]\w*?)`?(?:\s+((?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)(\s+UNSIGNED\s+)?(.*?)?(?:,|\)|$))/is', trim($column_list), $Columns);

				if(!$Columns) {
					return $this->_set_error('Parsing error in CREATE TABLE query');
				}
				
				$new_columns = array();

				$numMatches = count($Columns[0]);
				for($c = 0; $c < $numMatches; $c++) {
					//$column = str_replace("\"", "'", $column);
					if($Columns[1][$c])
					{
						if(!$Columns[3][$c]) {
							return $this->_set_error("Parse Error: Excepted column name in \"{$Columns[1][$c]}\"");
						}
						
						$keytype = strtolower($Columns[1][$c]);
						if($keytype === 'index')
							$keytype = 'key';
						$keycolumns = explode(',', $Columns[3][$c]);
						foreach($keycolumns as $keycolumn)
						{
							$keycolumn = trim($keycolumn);
							if($new_columns[$keycolumn]['key'] !== 'p')
								$new_columns[$keycolumn]['key'] = $keytype{0}; 
						}
					}
					else
					{
						$name = $Columns[4][$c];
						$type = $Columns[5][$c];
						$options =  $Columns[8][$c];
						
						if(isset($new_columns[$name])) {
							return $this->_set_error("Column '{$name}' redefined");
						}
						
						$type = strtoupper($type);
						if(in_array($type, array('CHAR', 'VARCHAR', 'BINARY', 'VARBINARY', 'TEXT', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT', 'SET', 'BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB'))) {
							$type = FSQL_TYPE_STRING;
						} else if(in_array($type, array('BIT','TINYINT', 'SMALLINT','MEDIUMINT','INT','INTEGER','BIGINT'))) {
							$type = FSQL_TYPE_INTEGER;
						} else if(in_array($type, array('FLOAT','REAL','DOUBLE','DOUBLE PRECISION','NUMERIC','DEC','DECIMAL'))) {
							$type = FSQL_TYPE_FLOAT;
						} else {
							switch($type)
							{
								case 'DATETIME':
									$type = FSQL_TYPE_DATETIME;
									break;
								case 'DATE':
									$type = FSQL_TYPE_DATE;
									break;
								case 'ENUM':
									$type = FSQL_TYPE_ENUM;
									break;
								case 'TIME':
									$type = FSQL_TYPE_TIME;
									break;
								case 'TIMESTAMP':
									$type = FSQL_TYPE_TIMESTAMP;
									break;
								case 'YEAR':
									$type = FSQL_TYPE_YEAR;
									break;
								default:
									break;
							}
						}
						
						$null = (bool) !preg_match("/\s+not\s+null\b/i", $options);
						
						$auto = (bool) preg_match("/\s+AUTO_INCREMENT\b/i", $options);
						
						if($type === FSQL_TYPE_ENUM) {
							preg_match_all("/'.*?(?<!\\\\)'/", $Columns[6][$c], $values);
							$restraint = $values[0];
						} else {
							$restraint = NULL;
						}
				
						if(preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\2))/is", $options, $matches)) {
							$default = $matches[1];
							if(!$null && strcasecmp($default, 'NULL')) {
								if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
									if($type === FSQL_TYPE_INTEGER)
										$default = (int) $matches[2];
									else if($type === FSQL_TYPE_FLOAT)
										$default = (float) $matches[2];
									else if($type === FSQL_TYPE_ENUM) {
										if(in_array($default, $restraint))
											$default = array_search($default, $restraint) + 1;
										else
											$default = 0;
									}
									else
										$default = $matches[2];
								} else {
									if($type === FSQL_TYPE_INTEGER)
										$default = (int) $default;
									else if($type === FSQL_TYPE_FLOAT)
										$default = (float) $default;
									else if($type === FSQL_TYPE_ENUM) {
										$default = (int) $default;
										if($default < 0 || $default > count($restraint)) {
											return $this->_set_error('Numeric ENUM value out of bounds');
										}
									}
								}
							}
							else if(!strcasecmp($default, 'NULL')) {
								$default = null;
							}
						}
						else if($null)
							$default = null;
						else if($type === FSQL_TYPE_STRING)
							// The default for string types is the empty string 
							$default = '';
						else
							// The default for dates, times, and number types is 0
							$default = 0;
				
						if(preg_match('/(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)/is', $options, $keymatches)) {
							$keytype = strtolower($keymatches[1]);
							$key = $keytype{0}; 
						}
						else {
							$key = 'n';
						}
						
						$new_columns[$name] = array('type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint);
					}
				}
			} else {
				$src_table_name_pieces = $this->_parse_table_name($matches[5]);
				$src_table =& $this->_find_table($src_table_name_pieces);
				$src_schema =& $src_table->getSchema();
				if($src_table === false) {
					return false;
				} else if($src_table->exists()) {
					$new_columns = $src_table->getColumns();
				} else {
					return $this->_set_error("Table {$src_scchema->name}.{$src_table_name} doesn't exist");
				}
			}
			
			$schema->createTable($table->getName(), $new_columns, $temporary);
	
			return true;
		} else {
			return $this->_set_error('Invalid CREATE query');
		}
	}
	
	function _prep_for_insert($value)
	{
		if($value === null) {
			$value = 'NULL';
		}
		else if(is_string($value)) {
			$value = "'$value'";
		}
		return $value;
	}
	
	function _query_insert($query)
	{
		$this->affected = 0;

		// All INSERT/REPLACE queries are the same until after the table name
		if(preg_match('/\A((INSERT|REPLACE)(?:\s+(IGNORE))?\s+INTO\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?))\s+(.+?)\s*[;]?\Z/is', $query, $matches)) { 
			list(, $beginning, $command, $ignore, $table_name, $the_rest) = $matches;
		} else {
			return $this->_set_error('Invalid Query');
		}

		// INSERT...SELECT
		if(preg_match('/^(\(.*?\)\s*)?(SELECT\s+.+)/is', $the_rest, $is_matches)) {
			$insert_query = $beginning.' '.$is_matches[1].'VALUES(%s)';
			$id = $this->_query_select($is_matches[2]);
				
			while($values = $this->fetch_row($id)) {
				$values = array_map(array($this, '_prep_for_insert'), $values);
				$full_insert_query = sprintf($insert_query, implode(',', $values));
				$this->_query_insert($full_insert_query);
			}
			$this->free_result($id);
			unset ($id, $values);
			return TRUE;
		}
		
		$table_name_pieces = $this->_parse_table_name($table_name);
		$table =& $this->_find_table($table_name_pieces);
		if($table === false) {
			return false;
		} else if(!$table->exists()) {
			return $this->_error_table_not_exists($table_name_pieces);
		}
		elseif($table->isReadLocked()) {
			return $this->_error_table_read_lock($table_name_pieces);
		}

		$tableColumns = $table->getColumns();
		$tableCursor =& $table->getWriteCursor();

		$check_names = 1;
		$replace = !strcasecmp($command, 'REPLACE');
		$ignore = !empty($ignore);
		
		// Column List present and VALUES list
		if(preg_match('/^\(`?(.+?)`?\)\s+VALUES\s*\((.+)\)/is', $the_rest, $matches)) { 
			$Columns = preg_split('/`?\s*,\s*`?/s', $matches[1]);
			$get_data_from = $matches[2];
		}
		// VALUES list but no column list
		else if(preg_match('/^VALUES\s*\((.+)\)/is', $the_rest, $matches)) { 
			$get_data_from = $matches[1];
			$Columns = $table->getColumnNames();
			$check_names = 0;
		}
		// SET syntax
		else if(preg_match('/^SET\s+(.+)/is', $the_rest, $matches)) { 
			$SET = explode(',', $matches[1]);
			$Columns= array();
			$data_values = array();
			
			foreach($SET as $set) {
				list($column, $value) = explode('=', $set);
				$Columns[] = trim($column);
				$data_values[] = trim($value);
			}
			
			$get_data_from = implode(',', $data_values);
		} else {
			return $this->_set_error('Invalid Query');
		}

		preg_match_all("/\s*(DEFAULT|AUTO|NULL|'.*?(?<!\\\\)'|(?:[\+\-]\s*)?\d+(?:\.\d+)?|[^$])\s*(?:$|,)/is", $get_data_from, $newData);
		$dataValues = $newData[1];
	
		if($check_names === 1) {
			if(count($dataValues) != count($Columns)) {
				return $this->_set_error("Number of inserted values and columns not equal");
			}

			$dataValues = array_combine($Columns, $newData[1]);
			$TableColumns = $table->getColumnNames();

			foreach($TableColumns as $col_index => $col_name) {
				if(!in_array($col_name, $Columns)) {
					$Data[$col_index] = "NULL";
				} else {
					$Data[$col_index] = $dataValues[$col_name];
				}
			}

			foreach($Columns as $col_name) {
				if(!in_array($col_name, $TableColumns)) {
					return $this->_set_error("Invalid column name '{$col_name}' found");
				}
			}
		}
		else
		{
			$countData = count($dataValues);
			$countColumns = count($Columns);
			
			if($countData < $countColumns) { 
				$Data = array_pad($dataValues, $countColumns, "NULL");
			} else if($countData > $countColumns) { 
				return $this->_set_error("Trying to insert too many values");
			} else {
				$Data = $dataValues;
			}
		}
		
		$unique_keys = array(0 => array('type' => 'p', 'columns' => array()));
		$newentry = array();
		$col_index = -1;
		
		////Load Columns & Data for the Table
		foreach($tableColumns as $col_name => $columnDef)  {

			++$col_index;
			
			$data = trim($Data[$col_index]);				
			$data = strtr($data, array("$" => "\$", "\$" => "\\\$"));
			
			////Check for Auto_Increment
			if((!strcasecmp($data, 'NULL') || strlen($data) === 0 || !strcasecmp($data, 'AUTO')) && $columnDef['auto']) {
				$tableCursor->last();
				$lastRow = $tableCursor->getRow();
				if($lastRow !== NULL)
					$this->insert_id = $lastRow[$col_index] + 1;
				else
					$this->insert_id = 1;
				$newentry[$col_index] = $this->insert_id;
			}
			else
			{
				$data = $this->_parse_value($columnDef, $data);
				if($data === false)
					return false;
				$newentry[$col_index] = $data;
			}
			
			////See if it is a PRIMARY KEY or UNIQUE
			if($columnDef['key'] === 'p')
				$unique_keys[0]['columns'][] = $col_index;
			else if($columnDef['key'] === 'u')
				$unique_keys[] = array('type' => 'u', 'columns' => array($col_index));	
		}
		
		if(!empty($unique_keys[0]['columns']) || count($unique_keys) > 1) {
			$tableCursor->first();
			while(!$tableCursor->isDone()) {
				$row = $tableCursor->getRow();
				$do_delete = false;
				foreach($unique_keys as $unique_key) {
					$match_found = true;
					foreach($unique_key['columns'] as $col_index) {
						$match_found = $match_found && $row[$col_index] == $newentry[$col_index];
					}
					if($match_found) {
						if($replace)
							$do_delete = true;
						else if(!$ignore)
							return $this->_set_error("Duplicate value found on key");
						else
							return true;
					}
				}
				
				if($do_delete) {
					$tableCursor->deleteRow();
					$this->affected++;
				}
				else
					$tableCursor->next();
			}
		}

		$tableCursor->appendRow($newentry);
		
		if($this->auto)
			$table->commit();
		else if(!in_array($table, $this->updatedTables))
			$this->updatedTables[] =& $table;

		$this->affected++;
		
		return true;
	}
	
	////Update data in the DB
	function _query_update($query) {
		$this->affected = 0;
		if(preg_match('/\AUPDATE(?:\s+(IGNORE))?\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+SET\s+(.*)(?:\s+WHERE\s+.+)?\s*[;]?\Z/is', $query, $matches)) {
			list(, $ignore, $table_name, $set_clause) = $matches;
			$ignore = !empty($ignore);
			$set_clause = preg_replace('/(.+?)(\s+WHERE\s+)(.*)/is', '\\1', $set_clause);

			$table_name_pieces = $this->_parse_table_name($table_name);
			$table =& $this->_find_table($table_name_pieces);
			if($table === false) {
				return false;
			} else if(!$table->exists()) {
				return $this->_error_table_not_exists($table_name_pieces);
			}
			elseif($table->isReadLocked()) {
				return $this->_error_table_read_lock($table_name_pieces);
			}
		
			$columns = $table->getColumns();
			$columnNames = array_keys($columns);
			$cursor =& $table->getWriteCursor();

			if(preg_match_all("/`?((?:\S+)`?\s*=\s*(?:'(?:.*?)'|\S+))`?\s*(?:,|\Z)/is", $set_clause, $sets)) {
				foreach($sets[1] as $set) {
					$s = preg_split('/`?\s*=\s*`?/', $set);
					$SET[] = $s;
					if(!isset($columns[$s[0]])) {
						return $this->_set_error("Invalid column name '{$s[0]}' found");
					}
				}
			}
			else
				$SET[0] =  preg_split('/\s*=\s*/', $set_clause);

			$where = null;
			if(preg_match('/\s+WHERE\s+((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/is', $query, $sets))
			{
				$where = $this->_build_where($sets[1], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
				if(!$where) {
					return $this->_set_error('Invalid/Unsupported WHERE clause');
				}
			}
			
			$col_indicies = array_flip($columnNames);
			$updates = array();
			
			$code = "";
			foreach($SET as $set) {
				list($column, $value) = $set;	
				$columnDef = $columns[$column];
				$new_value = $this->_parse_value($columnDef, $value);
				if($new_value === false)
					return $this->_set_error('Unknown value: '.$value);
				if(is_string($new_value))
					$new_value = $this->_prep_for_insert($new_value);
				$col_index = $col_indicies[$column];
				$updates[$col_index] = $new_value;
				$code .= "\t\t\$cursor->updateField($col_index, $new_value);\r\n";
			}
			
			$code .= "\t\t\$this->affected++;\r\n";
			
			if($where)
				$code = "\tif($where) {\r\n$code\r\n\t}";

			eval(<<<EOC
for( \$cursor->first(); !\$cursor->isDone(); \$cursor->next())
{
	\$entry = \$cursor->getRow();
$code
}
EOC
			);

			if($this->affected)
			{
				if($this->auto)
					$table->commit();
				else if(!in_array($table, $this->updatedTables))
					$this->updatedTables[] =& $table;
			}
			
			return TRUE;
		} else {
			return $this->_set_error('Invalid UPDATE query');
		}
	}
 
	////Select data from the DB
	function _query_select($query)
	{
		$selects = preg_split('/\s+UNION\s+/i', $query);
		$e = 0;
		foreach($selects as $select)
		{
			$matches = array();
			$tables = array();
			$simple = true;
			$distinct = 0;
			if(preg_match('/(.+?)\s+(?:WHERE|(?:GROUP|ORDER)\s+BY|LIMIT)\s+(.+?)/is',$select)) {
				$simple = false;
				preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.+?)\s+FROM\s+(.+?)\s+(?:WHERE|(?:GROUP|ORDER)\s+BY|LIMIT)\s+/is', $select, $matches);
				$matches[4] = preg_replace('/(.+?)\s+(WHERE|(?:GROUP|ORDER)\s+BY|LIMIT)\s+(.+?)/is', '\\1', $matches[4]);
			}
			else if(preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*?)\s+FROM\s+(.+)/is', $select, $matches)) { /* I got the matches, do nothing else */ }
			else { preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*)/is', $select, $matches); $matches[4] = "FSQL"; }

			$isTableless = !strcasecmp($matches[4], 'FSQL');
			$distinct = !strncasecmp($matches[1], 'DISTINCT', 8);
			$has_random = $matches[2] !== ' ';
			
			//expands the tables and loads their data
			$tbls = explode(',', $matches[4]);
			$joins = array();
			$joined_info = array( 'tables' => array(), 'offsets' => array(), 'columns' =>array() );
			if(!$isTableless)
			{
				foreach($tbls as $table_name) {
					if(preg_match('/\A\s*(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(.*)/is', $table_name, $tbl_data)) {
						list(, $table_name, $the_rest) = $tbl_data;
						
						$table_name_pieces = $this->_parse_table_name($table_name);
						$table =& $this->_find_table($table_name_pieces);
						if($table == false)
							return false;
						else if(!$table->exists())
							return $this->_error_table_not_exists($table_name_pieces);
						
						$schema = $table->getSchema();
						$saveas = $schema->getDatabase()->getName().'.'.$schema->getName().'.'.$table_name;
	
						if(preg_match('/\A\s+(?:AS\s+)?([^\W\d]\w*)(.*)/is', $the_rest, $alias_data)) {
							if(!in_array(strtolower($alias_data[1]), array('natural', 'left', 'right', 'full', 'outer', 'cross', 'inner')))
								list(, $saveas, $the_rest) = $alias_data;
						}
					} else {
						return $this->_set_error('Invalid table list');
					}
				
					if(!isset($tables[$saveas]))
						$tables[$saveas] =& $table;
					else
						return $this->_set_error("Table named '$saveas' already specified");
	
					$joins[$saveas] = array();
					$table_columns = $table->getColumns();
					$joined_info['tables'][$saveas] = $table_columns;
					$joined_info['offsets'][$saveas] = count($joined_info['columns']);
					$joined_info['columns'] = array_merge($joined_info['columns'], array_keys($table_columns));
	
					if(!empty($the_rest)) {
						preg_match_all('/((?:(?:NATURAL\s+)?(?:LEFT|RIGHT|FULL)(?:\s+OUTER)?|NATURAL|INNER|CROSS)\s+)?JOIN\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(?:AS\s+)?([^\W\d]\w*)(?=\s*\Z|\s+(?:USING|ON|INNER|NATURAL|CROSS|LEFT|RIGHT|FULL|JOIN)))?(?:\s+(USING|ON)\s*(?:(?:\((.*?)\))|(?:(?:\()?((?:\S+)\s*=\s*(?:\S+)(?:\))?))))?/is', $the_rest, $join);
						$numJoins = count($join[0]);
						for($i = 0; $i < $numJoins; ++$i) {
							$join_name = strtoupper($join[1][$i]);
							$is_natural = strpos($join_name, 'NATURAL') !== false;
						
							if(strpos($join_name, 'LEFT') !== false)
								$join_type = FSQL_JOIN_LEFT;
							else if(strpos($join_name, 'RIGHT') !== false)
								$join_type = FSQL_JOIN_RIGHT;
							else if(strpos($join_name, 'FULL') !== false)
								$join_type = FSQL_JOIN_FULL;
							else
								$join_type = FSQL_JOIN_INNER;

							$join_table_name_pieces = $this->_parse_table_name($join[2][$i]);
							$join_table =& $this->_find_table($join_table_name_pieces);
							if($join_table === false)
								return false;
							else if(!$join_table->exists())
								return $this->_error_table_not_exists($join_table_name_pieces);
	
							$join_table_name = $join_table->getName();
							$join_table_alias = !empty($join[3][$i]) ? $join[3][$i] : $join_table_name;
							if(!isset($tables[$join_table_alias]))
								$tables[$join_table_alias] = $join_table;
							else
								return $this->_set_error("Table named '$join_table_alias' already specified");
							
							$join_table_columns = $join_table->getColumns();
							$join_table_column_names = array_keys($join_table_columns);
	
							$clause = strtoupper($join[4][$i]);
							if($clause === 'USING' || !$clause && $is_natural) {
								if($clause)   // USING
									$shared_columns = preg_split('/\s*,\s*/', trim($join[5][$i]));
								else  // NATURAL
									$shared_columns = array_intersect($joined_info['columns'], $join_table_column_names);
								
								$conditional = '';
								foreach($shared_columns as $shared_column) {
									$conditional .= " AND {{left}}.$shared_column=$join_table_alias.$shared_column";
								}
								$conditions = substr($conditional, 5);
							}
							else if($clause === 'ON') {
								$conditions = trim($join[6][$i]);
							}
	
							$joined_info['tables'][$join_table_alias] = $join_table_columns;
							$new_offset = count($joined_info['columns']);
							$joined_info['columns'] = array_merge($joined_info['columns'], $join_table_column_names);
	
							$conditional = $this->_build_where($conditions, $joined_info, FSQL_WHERE_ON);
							if(!$conditional) {
								return $this->_set_error('Invalid/Unsupported WHERE clause');
							}
							
							if(!isset($this->join_lambdas[$conditional])) {
								$join_function = create_function('$left_entry,$right_entry', "return $conditional;");
								$this->join_lambdas[$conditional] = $join_function;
							}
							else
								$join_function = $this->join_lambdas[$conditional];
	
							$joined_info['offsets'][$join_table_alias] = $new_offset;
	
							$joins[$saveas][] = array('table' => $join_table_alias, 'type' => $join_type, 'clause' => $clause, 'comparator' => $join_function);
						}
					}
				}
				
				$data = array();
				foreach($joins as $base_table_name => $join_ops) {
					$base_table =& $tables[$base_table_name];
					$join_columns_size = count($base_table->getColumnNames());
					$join_data = $table->getEntries();
					foreach($join_ops as $join_op) {
						$joining_table =& $tables[$join_op['table']];
						$joining_columns_size = count($joining_table->getColumnNames());
	
						switch($join_op['type'])
						{
							default:
								$join_data = $this->_inner_join($join_data, $joining_table->getEntries(), $join_op['comparator']);
								break;
							case FSQL_JOIN_LEFT:
								$join_data = $this->_left_join($join_data, $joining_table->getEntries(), $join_op['comparator'], $joining_columns_size);
								break;
							case FSQL_JOIN_RIGHT:
								$join_data = $this->_right_join($join_data, $joining_table->getEntries(), $join_op['comparator'], $join_columns_size);
								break;
							case FSQL_JOIN_FULL:
								$join_data = $this->_full_join($join_data, $joining_table->getEntries(), $join_op['comparator'], $join_columns_size, $joining_columns_size);
								break;
						}
	
						$join_columns_size += $joining_columns_size;
					}
	
					// implicit CROSS JOINs
					if(!empty($join_data)) {					
						if(!empty($data)) {
							$new_data = array();
							foreach($data as $left_entry)
							{
								foreach($join_data as $right_entry) {
									$new_data[] = array_merge($left_entry, $right_entry);
								}
							}
							$data = $new_data;
						}
						else
							$data = $join_data;
					}
				}
			}
			
			preg_match_all("/(?:\A|\s*)((?:(?:-?\d+(?:\.\d+)?)|(?:[^\W\d]\w*\s*\(.*?\))|(?:(?:(?:[^\W\d]\w*)\.)?(?:(?:[^\W\d]\w*)|\*)))(?:\s+(?:AS\s+)?[^\W\d]\w*)?)\s*(?:\Z|,)/is", trim($matches[3]), $Columns);
			
			$ColumnList = array();
			$selectedInfo = array();
			foreach($Columns[1] as $column) {		
				if(preg_match('/\A((?:[^\W\d]\w*)\s*\((?:.*?)?\))(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is', $column, $colmatches)) {
					$function_call = $colmatches[1];
					$alias = !empty($colmatches[2]) ? $colmatches[2] : $function_call;
					$ColumnList[] = $alias;
					$selectedInfo[] = array('function', $function_call);
				}
				else if(preg_match('/\A(?:([^\W\d]\w*)\.)?((?:[^\W\d]\w*)|\*)(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is',$column, $colmatches)) {
					list(, $table_name, $column) = $colmatches;
					if($column === '*') {
						if(isset($colmatches[3])) 
							return $this->_set_error('Unexpected alias after "*"');

						if(!empty($table_name)) {
							$start_index = $joined_info['offsets'][$table_name];
							$column_names = $tables[$table_name]->getColumnNames();
							$ColumnList = array_merge($ColumnList, $column_names);
							foreach($column_names as $index => $column_name) {
								$selectedInfo[] = array('column', $start_index + $index);
							}
						} else {
							foreach(array_keys($tables) as $tname) {
								$start_index = $joined_info['offsets'][$tname];
								$column_names = $tables[$tname]->getColumnNames();
								$ColumnList = array_merge($ColumnList, $column_names);
								foreach($column_names as $index => $column_name) {
									$selectedInfo[] = array('column', $start_index + $index);
								}
							}
						}
					} else {
						if($table_name) {
							$index = array_search($column, $tables[$table_name]->getColumnNames()) + $joined_info['offsets'][$table_name];
						} else {
							$index = array_search($column, $joined_info['columns']);
						}
						
						$selectedInfo[] = array('column', $index);
						
						if(!empty($colmatches[3])) {
							$ColumnList[] = $colmatches[3];
						} else {
							$ColumnList[] = $column;
						}
					}
				}
				else if(preg_match("/\A(-?\d+(?:\.\d+)?)(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is", $column, $colmatches)) {
					$value = $colmatches[1];
					if(!empty($colmatches[2])) {
						$ColumnList[] = $colmatches[2];
					} else {
						$ColumnList[] = $value;
					}
					$selectedInfo[] = array('number', $value);
				}
				else {
					$ColumnList[] = $column;
				}
			}

			$limit = null;
			$tosort = array();
			$group_list = array();
			$having_clause = null;
			$where = null;
			
			if(!$simple)
			{
				if(preg_match('/\s+LIMIT\s+(?:(?:(\d+)\s*,\s*(\-1|\d+))|(\d+))/is', $select, $additional)) {
					list(, $limit_start, $limit_stop) = $additional;
					if($additional[3]) { $limit_stop = $additional[3]; $limit_start = 0; }
					else if($additional[2] != -1) { $limit_stop += $limit_start; }
					$limit = array($limit_start, $limit_stop);
				}
				
				if(preg_match('/\s+ORDER\s+BY\s+(?:(.*)\s+LIMIT|(.*))?/is', $select, $additional)) {
					if(!empty($additional[1])) { $ORDERBY = explode(',', $additional[1]); }
					else { $ORDERBY = explode(',', $additional[2]); }
					for($i = 0; $i < count($ORDERBY); ++$i) {
						if(preg_match('/([^\W\d]\w*)(?:\s+(ASC|DESC))?/is', $ORDERBY[$i], $additional)) {
							$index = array_search($additional[1], $joined_info['columns']);
							if(empty($additional[2])) { $additional[2] = 'ASC'; }
							$tosort[] = array('key' => $index, 'ascend' => !strcasecmp("ASC", $additional[2]));
						}
					}
				}

				if(preg_match('/\s+GROUP\s+BY\s+(?:(.*)\s+(?:HAVING|ORDER\s+BY|LIMIT)|(.*))?/is', $select, $additional)) {
					$group_clause = !empty($additional[1]) ? $additional[1] : $additional[2];
					$GROUPBY = explode(',', $group_clause);
					foreach($GROUPBY as $group_item)
					{
						if(preg_match('/([^\W\d]\w*)(?:\s+(ASC|DESC))?/is', $group_item, $additional)) {
							$index = array_search($additional[1], $joined_info['columns']);
							if(empty($additional[2])) { $additional[2] = 'ASC'; }
							$group_list[] = array('key' => $index, 'ascend' => !strcasecmp("ASC", $additional[2]));
						}
					}
				}
				
				if(preg_match('/\s+HAVING\s+((?:.+)(?:(?:((?:\s+)(?:AND|OR)(?:\s+))?(?:.+)?)*)?)(?:\s+(?:ORDER\s+BY|LIMIT))?/is', $select, $additional)) {
					$having_clause = $additional[1];
				}
				
				if(preg_match('/\s+WHERE\s+((?:.+)(?:(?:((?:\s+)(?:AND|OR)(?:\s+))?(?:.+)?)*)?)(?:\s+(?:(?:GROUP|ORDER)\s+BY|LIMIT))?/is', $select, $first_where)) {
					$where = $this->_build_where($first_where[1], $joined_info);
					if(!$where) {
						return $this->_set_error('Invalid/Unsupported WHERE clause');
					}
				}
			}
			
			$group_key = NULL;
			$final_code = NULL;
			if(!empty($group_list))
			{
				$joined_info['group_columns'] = array();
				
				if(count($group_list) === 1)
				{
					$group_col = $group_list[0]['key'];
					$group_key = '$entry[' . $group_col .']';
					$group_array = array($group_key);
					$joined_info['group_columns'][] = $group_col;
				}
				else
				{
					$all_ascend = 1;
					$group_array = array();
					$group_key_list = '';
					foreach($group_list as $group_item)
					{
						$all_ascend &= (int) $group_item['ascend'];
						$group_col = $group_item['key'];
						$group_array[] = $group_col;
						$group_key_list .= '$entry[' . $group_col .'], ';
						$joined_info['group_columns'][] = $group_col;
					}
					$group_key = 'serialize(array('. substr($group_key_list, 0, -2) . '))';
				}
				
				$select_line = "";
				foreach($selectedInfo as $info) {
					if($info[0] === 'column') {
						$column = $info[1];
						if(in_array($column, $group_array)) {
							$select_line .= '$group[0][' . $column .'], ';
						} else {
							return $this->_set_error("Selected column '{$joined_info['columns'][$column]}' is not a grouped column");
						}
					}
					else if($info[0] === 'number') {
						$select_line .= $info[1].', ';
					}
					else if($info[0] === 'function') {
						$expr = $this->_build_expr($info[1], $joined_info);
						$select_line .= $expr['expression'].', ';
					}
				}
				
				$line = '$grouped_set['.$group_key.'][] = $entry;';
				$final_line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
				$grouped_set = array();
				
				if($having_clause !== null) {
					$having = $this->_build_where($having_clause, $joined_info, FSQL_WHERE_HAVING);
					if(!$having) {
						return $this->_set_error('Invalid/Unsupported HAVING clause');
					}
					$final_line = "if($having) {\r\n\t\t\t\t\t$final_line\r\n\t\t\t\t}";
				}
				
				$final_code = <<<EOT
			foreach(\$grouped_set as \$group) {
				$final_line
			}
EOT;
			}
			else
			{
				$select_line = "";
				foreach($selectedInfo as $info) {
					switch($info[0]) {
						case  'column':
							$select_line .= '$entry[' . $info[1] .'], ';
							break;
						case 'number':
							$select_line .= $info[1].', ';
							break;
						case 'function':
							$expr = $this->_build_expr($info[1], $joined_info, false);
							$select_line .= $expr['expression'].', ';
							break;
					}
				}
				$line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
				if(!$isTableless)
					$group = $data;
			}
			
			if(!$isTableless) {
				if($where !== null)
					$line = "if($where) {\r\n\t\t\t\t\t$line\r\n\t\t\t\t}";
				
				$code = <<<EOT
				foreach(\$data as \$entry) {
					$line
				}
				
$final_code
EOT;
			}
			else
				$code = $line;
			
			$final_set = array();
			eval($code);
						
			// Execute an ORDER BY
			if(!empty($tosort))
			{
				$order = new fSQLOrderByClause($tosort);
				$order->sort($final_set);
			}
			
			// Execute a LIMIT
			if($limit !== null)
				$final_set = array_slice($final_set, $limit[0], $limit[1]);
		}
		
		return new fSQLResultSet(array_values($ColumnList), $final_set);
	}

	function _cross_product($left_data, $right_data)
	{
		if(empty($left_data) || empty($right_data))
			return array();

		$new_join_data = array();

		foreach($left_data as $left_entry)
		{
			foreach($right_data as $right_entry) {
				$new_join_data[] = array_merge($left_entry, $right_entry);
			}
		}

		return $new_join_data;
	}

	function _inner_join($left_data, $right_data, $join_comparator)
	{
		if(empty($left_data) || empty($right_data))
			return array();

		$new_join_data = array();

		foreach($left_data as $left_entry)
		{
			foreach($right_data as $right_entry) {
				if($join_comparator($left_entry, $right_entry)) {
					$new_join_data[] = array_merge($left_entry, $right_entry);
				}
			}
		}

		return $new_join_data;
	}

	function _left_join($left_data, $right_data, $join_comparator, $pad_length)
	{
		$new_join_data = array();
		$right_padding = array_fill(0, $pad_length, NULL);

		foreach($left_data as $left_entry)
		{
			$match_found = false;
			foreach($right_data as $right_entry) {
				if($join_comparator($left_entry, $right_entry)) {
					$match_found = true;
					$new_join_data[] = array_merge($left_entry, $right_entry);
				}
			}

			if(!$match_found) 
				$new_join_data[] = array_merge($left_entry, $right_padding);
		}

		return $new_join_data;
	}

	function _right_join($left_data, $right_data, $join_comparator, $pad_length)
	{
		$new_join_data = array();
		$left_padding = array_fill(0, $pad_length, NULL);

		foreach($right_data as $right_entry)
		{
			$match_found = false;
			foreach($left_data as $left_entry) {
				if($join_comparator($left_entry, $right_entry)) {
					$match_found = true;
					$new_join_data[] = array_merge($left_entry, $right_entry);
				}
			}

			if(!$match_found) 
				$new_join_data[] = array_merge($left_padding, $right_entry);
		}

		return $new_join_data;
	}

	function _full_join($left_data, $right_data, $join_comparator, $left_pad_length, $right_pad_length)
	{
		$new_join_data = array();
		$matched_rids = array();
		$left_padding = array_fill(0, $left_pad_length, NULL);
		$right_padding = array_fill(0, $right_pad_length, NULL);

		foreach($left_data as $left_entry)
		{
			$match_found = false;
			foreach($right_data as $rid => $right_entry) {
				if($join_comparator($left_entry, $right_entry)) {
					$match_found = true;
					$new_join_data[] = array_merge($left_entry, $right_entry);
					if(!in_array($rid, $matched_rids))
						$matched_rids[] = $rid;
				}
			}

			if(!$match_found) 
				$new_join_data[] = array_merge($left_entry, $right_padding);
		}

		$unmatched_rids = array_diff(array_keys($right_data), $matched_rids);
		foreach($unmatched_rids as $rid) {
			$new_join_data[] = array_merge($left_padding, $right_data[$rid]);
		}

		return $new_join_data;
	}

	function _build_where($statement, $join_info, $where_type = FSQL_WHERE_NORMAL)
	{
		if($statement) {
			preg_match_all("/(\A\s*|\s+(?:AND|OR)\s+)(NOT\s+)?(\S+?)(\s*(?:!=|<>|>=|<=>?|>|<|=)\s*|\s+(?:IS(?:\s+NOT)?|(?:NOT\s+)?IN|(?:NOT\s+)?R?LIKE|(?:NOT\s+)?REGEXP)\s+)(\((.*?)\)|'.*?'|\S+)/is", $statement, $WHERE);
			
			$where_count = count($WHERE[0]);
			if($where_count === 0)
				return null;
			
			$condition = "";
						
			for($i = 0; $i < $where_count; ++$i) {
				$local_condition = "";
				$logicalOp = trim($WHERE[1][$i]);
				$not = !empty($WHERE[2][$i]);
				$leftStr = $WHERE[3][$i];
				$operator = preg_replace("/\s+/", " ", trim(strtoupper($WHERE[4][$i])));
				$rightStr = $WHERE[5][$i];
				
				$left = $this->_build_expr($leftStr, $join_info, $where_type);
				if($left === null)
					return null;
				
				$leftExpr = $left['expression'];
				
				if($operator !== "IN" && $operator !== 'NOT IN')
				{
					$right = $this->_build_expr($rightStr, $join_info, $where_type);
					if($right === null)
						return null;

					$rightExpr = $right['expression'];

					if($left['nullable'] && $right['nullable'])
						$nullcheck = "nullcheck";
					else if($left['nullable'])
						$nullcheck = "nullcheck_left";
					else if($right['nullable'])
						$nullcheck = "nullcheck_right";
					else
						$nullcheck = null;
					
					switch($operator) {
						case '=':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_eq($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '!=':
						case '<>':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_ne($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr != $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '>':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_gt($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr > $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '>=':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_ge($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr >= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_lt($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr < $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<=':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_le($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr <= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<=>':
							$local_condition .= "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case 'IS NOT':
							$not = !$not;
						case 'IS':
							if($rightExpr === 'NULL')
								$local_condition = "($leftExpr === NULL ? FSQL_TRUE : FSQL_FALSE)";
							else if($rightExpr === 'TRUE')
								$local_condition = "($leftExpr == TRUE ? FSQL_TRUE : FSQL_FALSE)";
							else if($rightExpr === 'FALSE')
								$local_condition = "(in_array($leftExpr, array(0, 0.0, ''), true) ? FSQL_TRUE : FSQL_FALSE)";
							else
								return null;
							break;
						case 'NOT LIKE':
							$not = !$not;
						case 'LIKE':
							$local_condition = "fSQLEnvironment::_fsql_like($leftExpr, $rightExpr)";
							break;
						case 'NOT RLIKE':
						case 'NOT REGEXP':
							$not = !$not;
						case 'RLIKE':
						case 'REGEXP':
							$local_condition = "fSQLEnvironment::_fsql_regexp($leftExpr, $rightExpr)";
							break;
						default:
							$local_condition = "$leftExpr $operator $rightExpr";
							break;
					}
				}
				else
				{
					if(!empty($WHERE[6][$i])) {
						$array_values = explode(',', $WHERE[6][$i]);
						$valuesExpressions = array();
						foreach($array_values as $value)
						{
							$valueExpr = $this->_build_expr(trim($value), $join_info, $where_type);
							$valuesExpressions[] = $valueExpr['expression'];
						}
						$valuesString = implode(',', $valuesExpressions);
						$local_condition = "fSQLEnvironment::_fsql_in($leftExpr, array($valuesString))";
						
						if($operator === 'NOT IN')
							$not = !$not;
					}
					else
						return null;
				}
				
				if(!strcasecmp($logicalOp, 'AND'))
					$condition .= ' & ';
				else if(!strcasecmp($logicalOp, 'OR'))
					$condition .= ' | ';
				
				if($not)
					$condition .= '$this->_fsql_not('.$local_condition.')';
				else
					$condition .= $local_condition;
			}
			return "($condition) === ".FSQL_TRUE;
		}
		return null;
	}
 
	function _build_expr($exprStr, $join_info, $where_type = FSQL_WHERE_NORMAL)
	{
		$nullable = true;
		$expr = null;

		// function call
		if(preg_match("/\A([^\W\d]\w*)\s*\((.*?)\)/is", $exprStr, $matches)) {
			$function = strtolower($matches[1]);
			$params = $matches[2];
			$final_param_list = '';
			$function_info = null;
			$paramExprs = array();
			
			if(isset($this->registered_functions[$function])) {
				$builtin = false;
				$function_type = FSQL_FUNC_NORMAL;
			} else if(($function_info = fSQLFunctions::getFunctionInfo($function)) !== null) {
				$builtin = true;
				$function_type = $function_info[0];
				switch($function_type)
				{
					case FSQL_FUNC_AGGREGATE:
						$paramExprs[] = '$group';
						break;
					case FSQL_FUNC_ENV:
						$paramExprs[] = '$this';
						break;
				}
			} else {
				$this->_set_error('Call to unknown SQL function');
				return null;
			}

			if(!empty($params)) {
				$parameter = explode(',', $params);
				foreach($parameter as $param) {
					$param = trim($param);
					if($function_type === FSQL_FUNC_AGGREGATE && $param === '*' )
					{
						$paramExprs[] = '"*"';
					}
					else
					{	
						$paramExpr = $this->_build_expr($param, $join_info, $where_type | 1);
						$pexp = $paramExpr['expression'];
						if($function_type === FSQL_FUNC_AGGREGATE && preg_match('/\\$entry\[(\d+)\]/', $pexp, $pexp_matches))
							$paramExprs[] = $pexp_matches[1];
						else
							$paramExprs[] = $pexp;
					}
				}
			}
			
			$final_param_list = implode(",", $paramExprs);

			if($builtin)
				$expr = "fSQLFunctions::$function($final_param_list)";
			else
				$expr = "$function($final_param_list)";
		}
		// column/alias/keyword
		else if(preg_match("/\A(?:([^\W\d]\w*)\.)?([^\W\d]\w*)\Z/is", $exprStr, $matches)) {
			list( , $table_name, $column) =  $matches;
			// table.column
			if($table_name) {
				if(isset($join_info['tables'][$table_name])) {
					$table_columns = $join_info['tables'][$table_name];
					if(isset($table_columns[ $column ])) {
						$nullable = $table_columns[ $column ]['null'];
						if( isset($join_info['offsets'][$table_name]) ) {
							$colIndex = array_search($column,  array_keys($table_columns)) + $join_info['offsets'][$table_name];
							$expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
						} else {
							$colIndex = array_search($column, array_keys($table_columns));
							$expr = "\$right_entry[$colIndex]";
						}
					}
				}
			}
			// null/unknown
			else if(!strcasecmp($exprStr, 'NULL')  || !strcasecmp($exprStr, 'UNKNOWN')) {
				$expr = 'NULL';
			}
			// true/false
			else if(!strcasecmp($exprStr, 'TRUE') || !strcasecmp($exprStr, 'FALSE')) {
				$expr = strtoupper($exprStr);
				$nullable = false;
			}
			else if($where_type === FSQL_WHERE_HAVING) { // column/alias in grouping clause
				$colIndex = array_search($column, $join_info['columns']);
				if(in_array($colIndex, $join_info['group_columns'])) {
					$owner_table_name = null;
					foreach($join_info['tables'] as $join_table_name => $join_table)
					{
						if($colIndex >= $join_info['offsets'][$join_table_name])
							$owner_table_name = $join_table_name;
						else
							break;
					}
					$nullable = $join_info['tables'][$owner_table_name][$column]['null'];
					$expr = "\$group[0][$colIndex]";
				}
			}
			else {  // column/alias
				$colIndex = array_search($column, $join_info['columns']);
				if($colIndex === false)
					return null;

				$owner_table_name = null;
				foreach($join_info['tables'] as $join_table_name => $join_table)
				{
					if($colIndex >= $join_info['offsets'][$join_table_name])
						$owner_table_name = $join_table_name;
					else
						break;
				}
				$nullable = $join_info['tables'][$owner_table_name][$column]['null'];
				$expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
			}
		}
		// number
		else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $exprStr)) {
			$expr = $exprStr;
			$nullable = false;
		}
		// string
		else if(preg_match("/\A'.*?(?<!\\\\)'\Z/is", $exprStr)) {
			$expr = $exprStr;
			$nullable = false;
		}
		else if(($where_type & FSQL_WHERE_ON) && preg_match("/\A{{left}}\.([^\W\d]\w*)/is", $exprStr, $matches)) {
			if(($colIndex = array_search($matches[1], $join_info['columns']))) {
				$expr = "\$left_entry[$colIndex]";
			}
		}
		else
			return null;
		
		return array('nullable' => $nullable, 'expression' => $expr);
	}
	
	function _parse_value($columnDef, $value)
	{
		// Blank, NULL, or DEFAULT values
		if(!strcasecmp($value, 'NULL') || strlen($value) === 0 || !strcasecmp($value, 'DEFAULT')) {
			return !$columnDef['null'] ? $columnDef['default'] : null;
		}

		switch($columnDef['type']) {
			case FSQL_TYPE_INTEGER:
				if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
					return (int) $matches[1];
				}
				else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
					return (int) $value;
				}
				else {
					return $this->_set_error('Invalid integer value for insert');
				}
			case FSQL_TYPE_FLOAT:
				if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
					return (float) $matches[1];
				}
				else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
					return (float) $value;
				}
				else {
					return $this->_set_error('Invalid float value for insert');
				}
			case FSQL_TYPE_ENUM:
				if(in_array($value, $columnDef['restraint'])) {
					return array_search($value, $columnDef['restraint']) + 1;
				} else if(is_numeric($value))  {
					$val = (int) $value;
					if($val >= 0 && $val <= count($columnDef['restraint']))
						return $val;
					else {
						return $this->_set_error('Numeric ENUM value out of bounds');
					}
				} else {
					return $columnDef['default'];
				}
			case FSQL_TYPE_DATE:
				list($year, $month, $day) = array('0000', '00', '00');
				if(preg_match("/\A'((?:[1-9]\d)?\d{2})-(0[1-9]|1[0-2])-([0-2]\d|3[0-1])(?: (?:[0-1]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d))?'\Z/is", $value, $matches)
				|| preg_match("/\A'((?:[1-9]\d)?\d{2})(0[1-9]|1[0-2])([0-2]\d|3[0-1])(?:(?:[0-1]\d|2[0-3])(?:[0-5]\d)(?:[0-5]\d))?'\Z/is", $value, $matches)) {
					list(, $year, $month, $day) = $matches;
				} else {
					list($year, $month, $day) = array('0000', '00', '00');
				}
				if(strlen($year) === 2)
					$year = ($year <= 69) ? 2000 + $year : 1900 + $year;
				return $year.'-'.$month.'-'.$day;
			default:
				if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $matches)) {
					return (string) $matches[1];
				} else {
					return $value;
				}
		}
	}
	
	function _parse_schema_name($name)
	{
		if(preg_match('/^(?:(`?)([^\W\d]\w*)\1\.)?([^\W\d]\w*)\3$/', $name, $matches))
		{
			$db_name = !empty($matches[2]) ? $matches[2] : null;
			return array($db_name,  $matches[4]);
		}
		else
			return $this->_set_error('Parse error in table name');
	}
	
	function _parse_table_name($name)
	{
		if(preg_match('/^(?:(`?)([^\W\d]\w*)\1\.)?(?:(`?)([^\W\d]\w*)\3\.)?(`?)([^\W\d]\w*)\5$/', $name, $matches))
		{
			if(!empty($matches[2]) && empty($matches[4])) {
				$db_name = null;
				$schema_name = $matches[2];
			} else if(empty($matches[2])) {
				$db_name = null;
				$schema_name = null;
			} else {
				$db_name = $matches[2];
				$schema_name = $matches[4];
			}
			return array($db_name, $schema_name, $matches[6]);
		}
		else
			return $this->_set_error('Parse error in table name');
	}
	
	////Delete data from the DB
	function _query_delete($query)
	{
		$this->affected  = 0;
		if(preg_match('/\ADELETE\s+FROM\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(WHERE\s+.+))?\s*[;]?\Z/is', $query, $matches)) {
			
			$table_name_pieces = $this->_parse_table_name($matches[1]);
		
			$table =& $this->_find_table($table_name_pieces);
			if($table === false)
				return false;
			else if(!$table->exists())
				return $this->_error_table_not_exists($matches[1]);
			elseif($table->isReadLocked())
				return $this->_error_table_read_lock($matches[1]);
			
			$table_name = $table->getName();
			$columns = $table->getColumns();
			$cursor =& $table->getWriteCursor();
			$columnNames = array_keys($columns);

			if($cursor->isDone())
				return true;
			
			if(isset($matches[2]) && preg_match('/^WHERE\s+((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/i', $matches[3], $first_where))
			{
				$where = $this->_build_where($first_where[1], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
				if(!$where) {
					return $this->_set_error('Invalid/Unsupported WHERE clause');
				}
				$where = "return ($where);";

				$col_indicies = array_flip($columnNames);
			
				while(!$cursor->isDone()) {
					$entry = $cursor->getRow();
					if(eval($where))
					{					
						$cursor->deleteRow();
						$this->affected++;
					}
					else
						$cursor->next();
				}
			} else {
				while(!$cursor->isDone()) {
					$cursor->deleteRow();
					$this->affected++;
				}
			}
			
			if($this->affected)
			{
				if($this->auto)
					$table->commit();
				else if(!in_array($table, $this->updatedTables))
					$this->updatedTables[] =& $table;
			}

			return true;
		} else {
			return $this->_set_error('Invalid DELETE query');
		}
	}
 
	function _query_alter($query)
	{
		if(preg_match('/\AALTER\s+TABLE\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.*)/is', $query, $matches)) {
			list(, $table_name, $changes) = $matches;
			
			$table_name_pieces = $this->_parse_table_name($table_name);
			$tableObj =& $this->_find_table($table_name_pieces);
			if($tableObj === false)
				return false;
			$schema = $tableObj->getSchema();
			if(!$tableObj->exists()) {
				return $this->_error_table_not_exists($table_name_pieces);
			}
			elseif($tableObj->isReadLocked()) {
				return $this->_error_table_read_lock($table_name_pieces);
			}
			$columns =  $tableObj->getColumns();
			
			preg_match_all('/(?:ADD|ALTER|CHANGE|DROP|RENAME).*?(?:,|\Z)/is', trim($changes), $specs);
			for($i = 0; $i < count($specs[0]); $i++) {
				if(preg_match('/\AADD\s+(?:CONSTRAINT\s+`?[^\W\d]\w*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/is', $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];
					
					foreach($columns as $name => $column) {
						if($column['key'] === 'p') {
							return $this->_set_error('Primary key already exists');
						}
					}
					
					$columnDef['key'] = 'p';
					$tableObj->setColumns($columns);
					
					return true;
				} else if(preg_match("/\ACHANGE(?:\s+(?:COLUMN))?\s+`?([^\W\d]\w*)`?\s+(?:SET\s+DEFAULT ((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\3))|DROP\s+DEFAULT)(?:,|;|\Z)/is", $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];
					if(isset($matches[2]))
						$default = $matches[2];
					else
						$default = 'NULL';
					
					if(!$columnDef['null'] && strcasecmp($default, 'NULL')) {
						if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
							if($columnDef['type'] === FSQL_TYPE_INTEGER)
								$default = (int) $matches[2];
							else if($columnDef['type'] === FSQL_TYPE_FLOAT)
								$default = (float) $matches[2];
							else if($columnDef['type'] === FSQL_TYPE_ENUM) {
								if(in_array($default, $columnDef['restraint']))
									$default = array_search($default, $columnDef['restraint']) + 1;
								else
									$default = 0;
							}
						} else {
							if($columnDef['type'] === FSQL_TYPE_INTEGER)
								$default = (int) $default;
							else if($columnDef['type'] === FSQL_TYPE_FLOAT)
								$default = (float) $default;
							else if($columnDef['type'] === FSQL_TYPE_ENUM) {
								$default = (int) $default;
								if($default < 0 || $default > count($columnDef['restraint'])) {
									return $this->_set_error('Numeric ENUM value out of bounds');
								}
							}
						}
					} else if(!$columnDef['null']) {
						if($columnDef['type'] === FSQL_TYPE_STRING)
							// The default for string types is the empty string 
							$default = "''";
						else
							// The default for dates, times, and number types is 0
							$default = 0;
					}
					
					$columnDef['default'] = $default;
					$tableObj->setColumns($columns);
					
					return true;
				} else if(preg_match('/\ADROP\s+PRIMARY\s+KEY/is', $specs[0][$i], $matches)) {
					$found = false;
					foreach($columns as $name => $column) {
						if($column['key'] === 'p') {
							$columns[$name]['key'] = 'n';
							$found = true;
						}
					}
					
					if($found) {
						$tableObj->setColumns($columns);
						return true;
					} else {
						return $this->_set_error('No primary key found');
					}
				}
				else if(preg_match('/\ARENAME\s+(?:TO\s+)?(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $specs[0][$i], $matches)) {
					$new_table_name_pieces = $this->_parse_table_name($matches[1]);
					$new_table =& $this->_find_table($new_table_name_pieces);
					if($new_table === false)
						return false;
					$new_schema =& $new_table->getSchema();
					if($new_table->exists()) {
						return $this->_set_error("Destination table {$new_schema->name}.{$new_table_name} already exists");
					}
				
					return $schema->renameTable($table->getName(), $new_table->getName(), $new_schema);
				}
				else {
					return $this->_set_error('Invalid ALTER query');
				}
			}
		} else {
			return $this->_set_error('Invalid ALTER query');
		}
	}

	function _query_rename($query)
	{
		if(preg_match('/\ARENAME\s+TABLE\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				list($old, $new) = preg_split('/\s+TO\s+/i', trim($table));
				
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $old, $table_parts)) {
					$old_table_name_pieces = $this->_parse_table_name($table_parts[1]);
					$old_table =& $this->_find_table($old_table_name_pieces);
				} else {
					return $this->_set_error('Parse error in table listing');
				}
				
				if(preg_match('/`?(?:([^\W\d]\w*)`?\.`?)?(?:([^\W\d]\w*)`?\.`?)?([^\W\d]\w*)`?/is', $new, $table_parts)) {
					$new_table_name_pieces = $this->_parse_table_name($table_parts[1]);
					$new_table =& $this->_find_table($new_table_name_pieces);
				} else {
					return $this->_set_error('Parse error in table listing');
				}
				
				if($old_table === false || $new_table === false)
					return false;
				
				$old_schema = $old_table->getSchema();
				if(!$old_table->exists()) {
					return $this->_error_table_not_exists($old_table_name_pieces);
				}
				elseif($old_table->isReadLocked()) {
					return $this->_error_table_read_lock($old_table_name_pieces);
				}
				
				$new_schema = $new_table->getSchema();
				if($new_table->exists()) {
					return $this->_set_error("Destination table {$new_schema->name}.{$new_table_name_pieces[2]} already exists");
				}
				
				return $old_schema->renameTable($old_table->getName(), $new_table->getName(), $new_schema);
			}
			return TRUE;
		} else {
			return $this->_set_error('Invalid RENAME query');
		}
	}
	
	function _query_drop($query)
	{
		if(preg_match('/\ADROP(?:\s+(TEMPORARY))?\s+TABLE(?:\s+(IF\s+EXISTS))?\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$temporary = !empty($matches[1]);
			$ifexists = !empty($matches[2]);
			$tables = explode(',', $matches[3]);
	
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $table_parts)) {
					$table_name_pieces = $this->_parse_table_name($table_parts[1]);
					$table =& $this->_find_table($table_name_pieces);
					if($table === false)
						return false;
					$schema =& $table->getSchema();
					if($table->isReadLocked()) {
						return $this->_error_table_read_lock($table_name_pieces);
					}

					$existed = $schema->dropTable($table->getName());
					if(!$ifexists && !$existed) {
						return $this->_error_table_not_exists($table_name_pieces); 
					}
				} else {
					return $this->_set_error('Parse error in table listing');
				}
			}
			return true;
		} else if(preg_match('/\ADROP\s+DATABASE(?:\s+(IF\s+EXISTS))?\s+`?([^\W\d]\w*)`?s*[;]?\Z/is', $query, $matches)) {
			$ifexists = !empty($matches[1]);
			$db_name = $matches[2];
			
			if(!$ifexists && !isset($this->databases[$db_name])) {
				return $this->_set_error("Database '{$db_name}' does not exist"); 
			} else if(!isset($this->databases[$db_name])) {
				return true;
			}
			
			$db =& $this->databases[$db_name];
	
			$tables = $db->listTables();
			
			foreach($tables as $table) {
				$db->dropTable($table);
			}
			
			unset($this->databases[$db_name]);
			
			return TRUE;
		} else {
			$this->_set_error('Invalid DROP query');
			return null;
		}
	}
	
	function _query_truncate($query)
	{
		if(preg_match('/\ATRUNCATE\s+TABLE\s+(.*)[;]?\Z/is', $query, $matches)) {
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $matches)) {
					$table_name_pieces = $this->_parse_table_name($matches[1]);
					$table =& $this->_find_table($table_name_pieces);
					if($table === false) {
						return false;
					} else if($table->exists()) {
						if($table->isReadLocked()) {
							return $this->_error_table_read_lock($table_name_pieces);
						}
						$columns = $table->getColumns();
						$table_name = $table->getName();
						$db->dropTable($table_name);
						$db->createTable($table_name, $columns);
					} else {
						return $this->_error_table_not_exists($table_name_pieces); 
					}
				} else {
					return $this->_set_error('Parse error in table listing');
				}
			}
		} else {
			return $this->_set_error('Invalid TRUNCATE query');
		}
		
		return true;
	}
	
	function _query_backup($query)
	{
		if(!preg_match("/\ABACKUP\s+TABLE\s+(.*?)\s+TO\s+'(.*?)'\s*[;]?\s*\Z/is", $query, $matches)) {
			if(substr($matches[2], -1) != "/")
				$matches[2] .= '/';
			
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $table_name_matches)) {
					$table_name_pieces = $this->_parse_table_name($table_name_matches[1]);
					$schema =& $this->_find_schema($table_name_pieces[0], $table_name_pieces[1]);
					if($schema === false)
						return false;
					
					$schema->copyTable($table_name_pieces[2], $db->getPath(), $matches[2]);
				} else {
					return $this->_set_error('Parse error in table listing');
				}
			}
		} else {
			return $this->_set_error('Invalid BACKUP Query');
		}
	}
	
	function _query_restore($query)
	{
		if(!preg_match("/\ARESTORE\s+TABLE\s+(.*?)\s+FROM\s+'(.*?)'\s*[;]?\s*\Z/is", $query, $matches)) {
			if(substr($matches[2], -1) !== '/')
				$matches[2] .= '/';
			
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $table_name_matches)) {
					$table_name_pieces = $this->_parse_table_name($table_name_matches[1]);
					$schema =& $this->_find_schema($table_name_pieces[0], $table_name_pieces[1]);
					if($schema === false)
						return false;
					
					$schema->copyTable($table_name_pieces[2], $matches[2], $db->getPath());
				} else {
					return $this->_set_error('Parse error in table listing');
				}
			}
		} else {
			return $this->_set_error('Invalid RESTORE Query');
		}
	}
 
	function _query_show($query)
	{
		if(preg_match('/\ASHOW\s+(FULL\s+)?TABLES(?:\s+(?:FROM|IN)\s+(`?(?:[^\W\d]\w*`?\.`?)?[^\W\d]\w*`?)(?:\s+ORDER\s+BY\s+(.*?))?\s*[;]?\s*\Z/is', $query, $matches)) {
			
			$full = !empty($matches[1]);
			$schema_name = isset($matches[2]) ? $matches[2] : null;
			$order_clause = isset($matches[3]) ? $matches[3] : null;
			
			$schema_name_pieces = $this->_parse_schema_name($schema_name);
			if($schema_name_pieces !== false) {
				$schema =& $this->_find_schema($schema_name_pieces[0], $schema_name_pieces[1]);
				if($schema === false)
					return false;
			}
			else
				return false;
		
			$tables = $schema->listTables();
			$data = array();
			
			$base = $full ? array('BASE TABLE') : array();
			foreach($tables as $table_name) {
				$data[] = array_merge(array($table_name), $base);
			}
			
			$columns = array('Tables_in_'.$schema->name);
			if($full)
				$columns[] = 'Table_type';
			
			$rs =  new fSQLResultSet($columns, $data);
			
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
					
					$order = new fSQLOrderByClause($tosort);
					$order->sort($data);
				}
			}
			
			return new fSQLResultSet($columns, $data);
		} else if(preg_match('/\ASHOW\s+DATABASES\s*[;]?\s*\Z/is', $query, $matches)) {
			
			$dbs = array_keys($this->databases);
			foreach($dbs as $db) {
				$data[] = array($db);
			}
			
			return new fSQLResultSet(array('Database'), $data);
		} else if(preg_match('/\ASHOW\s+(FULL\s+)?COLUMNS\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?(?:\s+(?:FROM|IN)\s+`?(?:([^\W\d]\w*)`?\.`?)?([^\W\d]\w*)`?)?\s*[;]?\s*\Z/is', $query, $matches)) {
			$db_name = isset($matches[3]) ? $matches[3] : null;
			$schema_name = isset($matches[4]) ? $matches[4] : null;
			return $this->_show_columns($db_name, $schema_name, $matches[2], !empty($matches[1]));
		} else {
			return $this->_set_error('Invalid SHOW query');
		}
	}
	
	function _show_columns($db_name, $schema_name, $table_name, $full)
	{
		$tableObj =& $this->_find_table($db_name, $schema_name, $table_name);
		if($tableObj === false)
			return false;
		else if(!$tableObj->exists())
			return $this->_error_table_not_exists($schema->name, $table_name);
		
		$columns =  $tableObj->getColumns();
			
		$data = array();
			
		foreach($columns as $name => $column) {
			$type = $this->_typecode_to_name($column['type']);
			$default = $column['default'];
			$null = ($column['null']) ? 'YES' : 'NO';
			$extra = ($column['auto']) ? 'auto_increment' : '';
			
			switch($column['key'])
			{
				case 'p':
					$key = 'PRI';
					break;
				case 'u':
					$key = 'UNI';
					break;
				default:
					$key = '';
					break;
			}

			if($full)
				$data[] = array($name, $type, null, $null, $default, $key, $extra,'select,insert,update,references','');
			else
				$data[] = array($name, $type, $null, $default, $key, $extra);
		}
		
		if($full)
			$columns = array('Field','Type','Collation','Null','Default','Key','Extra','Privileges','Comments');
		else
			$columns = array('Field','Type','Null','Default','Key','Extra');
			
			
		return new fSQLResultSet($columns, $data);		
	}
	
	function _query_describe($query)
	{
		if(preg_match('/\ADESC(?:RIBE)?\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s*[;]?\s*\Z/is', $query, $matches)) {
			$schema_name_pieces = $this->_parse_table_name($matches[1]);
			return $schema_name_pieces !== false ? $this->_show_columns($schema_name_pieces[0], $schema_name_pieces[1], $schema_name_pieces[2], false) : false;
		} else {
			return $this->_set_error('Invalid DESCRIBE query');
		}
	}
	
	function _query_use($query)
	{
		if(preg_match('/\AUSE\s+`?([^\W\d]\w*)`?\s*[;]?\s*\Z/is', $query, $matches)) {
			return $this->select_db($matches[1]);
		} else {
			return $this->_set_error('Invalid USE query');
		}
	}

	function _query_lock($query)
	{
		if(preg_match('/\ALOCK\s+TABLES\s+(.+?)\s*[;]?\s*\Z/is', $query, $matches)) {
			preg_match_all('/+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+((?:READ(?:\s+LOCAL)?)|((?:LOW\s+PRIORITY\s+)?WRITE))/is', $matches[1], $rules);
			$numRules = count($rules[0]);
			for($r = 0; $r < $numRules; $r++) {
				$table_name_pieces = $this->_parse_table_name($rules[1][$r]);
				$table =& $this->_find_table($table_name_pieces);
				if($table === false)
					return false;
				else if(!$table->exists())
					return $this->_error_table_not_exists($table_name_pieces);

				if(!strncasecmp($rules[4][$r], 'READ', 4)) {
					$table->readLock();
				}
				else {  /* WRITE */
					$table->writeLock();
				}

				$lockedTables[] =& $table;
			}
			return TRUE;
		} else {
			return $this->_set_error('Invalid LOCK query');
		}
	}

	function _query_unlock($query)
	{
		if(preg_match('/\AUNLOCK\s+TABLES\s*[;]?\s*\Z/is', $query)) {
			$this->_unlock_tables();
			return TRUE;
		} else {
			return $this->_set_error('Invalid UNLOCK query');
		}
	}
	
	function _is_valid_result_set(&$rs) {
		return is_object($rs) && !strcasecmp(get_class($rs), 'fSQLResultSet') && isset($rs->columns);
	}
	
	function fetch_all(&$rs, $type = 1)
	{
		if($this->_is_valid_result_set($rs)) {
			if($type === FSQL_NUM) {
				return $rs->data;
			}
			
			$result_array = array();
			$columns = $rs->columns;
			if($type === FSQL_ASSOC) {
				foreach($rs->data as $entry)
					$result_array[] = array_combine($columns, $entry);
			}
			else {
				foreach($rs->data as $entry)
					$result_array[] = array_merge($entry, array_combine($columns, $entry));
			}
			return $result_array;
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function fetch_array(&$rs, $type = 1)
	{
		if($this->_is_valid_result_set($rs)) {
			
			$entry = $rs->dataCursor->getRow();
			if(!$entry)
				return false;
		
			$rs->dataCursor->next();
	
			if($type === FSQL_ASSOC) {  return array_combine($rs->columns, $entry); }
			else if($type === FSQL_NUM) { return $entry; }
			else{ return array_merge($entry, array_combine($rs->columns, $entry)); }
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function fetch_assoc($results) { return $this->fetch_array($results, FSQL_ASSOC); }
	function fetch_row	($results) { return $this->fetch_array($results, FSQL_NUM); }
	function fetch_both	($results) { return $this->fetch_array($results, FSQL_BOTH); }
 
	function fetch_object(&$rs)
	{
		$row = $this->fetch_array($rs, FSQL_ASSOC);
		if($row === false)
			return false;

		$obj =& new stdClass();

		foreach($row as $key => $value)
			$obj->{$key} = $value;

		return $obj;
	}
	
	function data_seek(&$rs, $i)
	{
		if($this->_is_valid_result_set($rs)) {
			return $rs->dataCursor->seek($i);
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function num_rows(&$rs)
	{
		if($this->_is_valid_result_set($rs)) {
			return $rs->dataCursor->numRows();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function num_fields(&$rs)
	{
		if($rs->_is_valid_result_set($rs)) {
			return $rs->columnsCursor->numRows();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function fetch_field(&$rs, $i = NULL)
	{
		if($this->_is_valid_result_set($rs)) {
			$cursor =& $rs->columnsCursor;
			
			if($i !== NULL)
				$cursor->seek($i);
			
			$column_name = $cursor->getRow();
			if(!$column_name)
				return false;

			$cursor->next();
			$field = new stdClass();
			$field->name = $column_name;
			return $field;
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function field_seek(&$rs, $i)
	{
		if($this->_is_valid_result_set($rs)) {
			return $rs->columnsCursor->seek($i);
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}

	function free_result(&$rs)
	{
		if($this->_is_valid_result_set($rs)) {
			return $rs->free();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}

	function _fsql_not($x)
	{
		$c = ~$x & 3;
		return (($c << 1) ^ ($c >> 1)) & 3;
	}

	function _fsql_like($left, $right)
	{
		if($left !== null && $right !== null)
		{
			$right = strtr(preg_quote($right, "/"), array('_' => '.', '%' => '.*', '\_' => '_', '\%' => '%'));
			return (preg_match("/\A{$right}\Z/is", $left)) ? FSQL_TRUE : FSQL_FALSE;
		}
		else
			return FSQL_UNKNOWN;
	}
	
	function _fsql_in($needle, $haystack)
	{
		if($needle !== null)
		{
			return (in_array($needle, $haystack)) ? FSQL_TRUE : FSQL_FALSE;
		}
		else
			return FSQL_UNKNOWN;
	}
	
	function _fsql_regexp($left, $right)
	{
		if($left !== null && $right !== null)
			return (eregi($right, $left)) ? FSQL_TRUE : FSQL_FALSE;
		else
			return FSQL_UNKNOWN;
	}

	function _typecode_to_name($type)
	{
		switch($type)
		{
			case FSQL_TYPE_DATE:		return 'DATE';
			case FSQL_TYPE_DATETIME:	return 'DATETIME';
			case FSQL_TYPE_ENUM:		return 'ENUM';
			case FSQL_TYPE_FLOAT:		return 'DOUBLE';
			case FSQL_TYPE_INTEGER:		return 'INTEGER';
			case FSQL_TYPE_STRING:		return 'TEXT';
			case FSQL_TYPE_TIME:		return 'TIME';
			case FSQL_TYPE_TIMESTAMP:	return 'TIMESTAMP';
			case FSQL_TYPE_YEAR:		return 'YEAR';
			default:					return false;
		}
	}
}

?>