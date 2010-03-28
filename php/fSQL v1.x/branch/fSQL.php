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

define('FSQL_WHERE_NORMAL',2,TRUE);
define('FSQL_WHERE_NORMAL_AGG',3,TRUE);
define('FSQL_WHERE_ON',4,TRUE);
define('FSQL_WHERE_HAVING',8,TRUE);
define('FSQL_WHERE_HAVING_AGG',9,TRUE);

define('FSQL_TYPE_BOOLEAN','b',TRUE);
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

define('FSQL_EXTENSIONS_PATH', FSQL_INCLUDE_PATH.'/extensions');

require FSQL_INCLUDE_PATH.'/fSQLCursors.php';
require FSQL_INCLUDE_PATH.'/fSQLParser.php';
require FSQL_INCLUDE_PATH.'/fSQLQuery.php';
require FSQL_INCLUDE_PATH.'/fSQLUtilities.php';
require FSQL_INCLUDE_PATH.'/drivers/fSQLBaseDriver.php';
require FSQL_INCLUDE_PATH.'/drivers/fSQLMemoryDriver.php';
require FSQL_INCLUDE_PATH.'/drivers/fSQLMasterDriver.php';

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
	var $resultSets = array();
	var $parser;
	
	function fSQLEnvironment()
	{
		list($usec, $sec) = explode(' ', microtime());
		srand((float) $sec + ((float) $usec * 100000));
		
		$db =& new fSQLMasterDatabase($this, 'FSQL');
		$db->create();
		$this->databases['FSQL'] =& $db;
		
		$master =& $this->_get_master_schema();
		$master->addDatabase($db);
		
		$this->parser = new fSQLParser();
	}
	
	function define_db($name, $path)
	{
		$this->error_msg = null;
		
		if($path !== FSQL_MEMORY_DB_PATH) {
			require FSQL_INCLUDE_PATH.'/drivers/fSQLStandardDriver.php';
			$db =& new fSQLDatabase($this, $name, $path);
		}
		else {
			$db =& new fSQLMemoryDatabase($this, $name);
		} 
		
		$this->databases[$name] =& $db;
		if($db->create()) {
			$master =& $this->_get_master_schema();
			$master->addDatabase($this->databases[$name]);
			return true;
		}
		else {
			unset($this->databases[$name]);
			return false;
		}
	}
	
	function define_schema($db_name, $schema_name)
	{
		$this->error_msg = null;
		
		$db =& $this->_get_database($db_name);
		if($db !== false) {
			$schema =& $db->defineSchema($schema_name);
			if($schema !== false)
			{
				$master =& $this->_get_master_schema();
				$master->addSchema($schema);
				return true;
			}
		}
		
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
			return $this->_set_error("No database called {$db_name} found");
		}
	}
	
	function close()
	{
		$this->_unlock_tables();
		
		foreach(array_keys($this->resultSets) as $rs_id)
			$this->resultSets[$rs_id]->close();
		
		foreach (array_keys($this->databases) as $db_name)
			$this->databases[$db_name]->close();
		
		$this->resultSets = array();
		$this->databases = array();
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
	
	function enable_mysql_exstensions($enable)
	{
		if($enable && !is_a($this->parser, 'fSQLParserMySQL'))
		{
			if(!class_exists('fSQLParserMySQL'))
				require FSQL_EXTENSIONS_PATH.'/mysql/fSQLParserMySQL.php';
			$this->parser = new fSQLParserMySQL($this);
		}
		else if(!$enable && is_a($this->parser, 'fSQLParserMySQL'))
		{
			$this->parser = new fSQLParser($this);
		}
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
	
	function _build_table_name($table_name_pieces)
	{
		list($db_name, $schema_name, $table_name) = $table_name_pieces;
		if($db_name === null)
			$db_name = $this->currentDB->getName();
		if($schema_name === null)
			$schema_name = 'public';
		return $db_name.'.'.$schema_name.'.'.$table_name;
	}
	
	function _error_table_not_exists($table_name_pieces)
	{
		$table_name = $this->_build_table_name($table_name_pieces);
		return $this->_set_error("Table {$table_name} does not exist"); 
	}

	function _error_table_read_lock($table_name_pieces)
	{
		$table_name = $this->_build_table_name($table_name_pieces);
		return $this->_set_error("Table {$table_name} is locked for reading only"); 
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
				if(!$table)
					$this->_error_table_not_exists($name_pieces);
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
		$this->query_count++;
		$this->error_msg = null;
		
		$command = $this->parser->parse($this, $query);
		if($command === false)
			return false;
		
		$command->prepare();
		return $command->execute();
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
	
	function _create_result_set($columns, $entries)
	{
		$rs_id = !empty($this->resultSets) ? max(array_keys($this->resultSets)) + 1 : 1;
		$this->resultSets[$rs_id] =& new fSQLResultSet($columns, $entries);
		return $rs_id;
	}
	
	function &get_result_set($rs_id)
	{
		$rs = $this->_is_valid_result_set($rs_id) ? $this->resultSets[$rs_id] : false;
		return $rs;
	}
	
	function _is_valid_result_set($rs_id) {
		return $rs_id !== false && isset($this->resultSets[$rs_id]->columns);
	}
	
	function fetch_all($rs_id, $type = 1)
	{
		if($this->_is_valid_result_set($rs_id)) {
			$rs =& $this->resultSets[$rs_id];
			
			if($type === FSQL_NUM) {
				return $rs->data;
			}
			
			$result_array = array();
			$columns = $rs->columnNames;
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
	
	function fetch_array($rs_id, $type = 1)
	{
		if($this->_is_valid_result_set($rs_id)) {
			$rs =& $this->resultSets[$rs_id];

			$entry = $rs->dataCursor->getRow();
			if(!$entry)
				return false;
			
			$rs->dataCursor->next();

			if($type === FSQL_ASSOC) {  return array_combine($rs->columnNames, $entry); }
			else if($type === FSQL_NUM) { return $entry; }
			else{ return array_merge($entry, array_combine($rs->columnNames, $entry)); }
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function fetch_assoc($results) { return $this->fetch_array($results, FSQL_ASSOC); }
	function fetch_row	($results) { return $this->fetch_array($results, FSQL_NUM); }
	function fetch_both	($results) { return $this->fetch_array($results, FSQL_BOTH); }
 
	function fetch_object($rs_id)
	{
		$row = $this->fetch_array($rs_id, FSQL_ASSOC);
		if($row === false)
			return false;

		$obj =& new stdClass();

		foreach($row as $key => $value)
			$obj->{$key} = $value;

		return $obj;
	}
	
	function data_seek($rs_id, $i)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->dataCursor->seek($i);
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function num_rows($rs_id)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->dataCursor->numRows();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function num_fields($rs_id)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->columnsCursor->numRows();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function fetch_field($rs_id, $i = NULL)
	{
		if($this->_is_valid_result_set($rs_id)) {
			$cursor =& $this->resultSets[$rs_id]>columnsCursor;
			
			if($i !== NULL)
				$cursor->seek($i);
			
			$column = $cursor->getRow();
			if(!$column)
				return false;

			$key = $column['key'];
			$type = $column['type'];
			
			$cursor->next();
			$field = new stdClass();
			$field->name = $column['name'];
			$field->def = $column['default'];
			$field->non_null = $column['null'] ? 0 : 1;
			$field->primary_key = (int) ($key === 'p');
			$field->unique_key = (int) ($key === 'u');
			$field->multiple_key = (int) ($key === 'k');
			$field->numeric = ($type === FSQL_TYPE_INTEGER || $type === FSQL_TYPE_FLOAT) ? 1 : 0;
			$field->blob = 0;
			$field->type = $this->_typecode_to_name($type);
			$field->unsigned = 0;
			$field->zerofill = 0;
			return $field;
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	function field_seek($rs_id, $i)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->columnsCursor->seek($i);
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}

	function free_result($rs_id)
	{
		if($this->_is_valid_result_set($rs_id)) {
			$ret = $this->resultSets[$rs_id]->free();
			unset($this->resultSets[$rs_id]);
			return $ret;
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