<?php
/**
 * fSQL start file
 * 
 * This file is the start point of the fSQL.
 * @author Kaja Fumei <???@???.???>
 * @version ??
 * @package fsql
 */

/**#@+
 * Constants
 */
/**
 * Defines the library version
 */
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

/**
 * fSQL library include path is set to same directory with
 * calling file in default.  
 */
if(!defined('FSQL_INCLUDE_PATH')) {
	define('FSQL_INCLUDE_PATH', dirname(__FILE__));
}

/**
 * fSQL library's extensions path is given relatively to FSQL_INCLUDE_PATH.
 */
define('FSQL_EXTENSIONS_PATH', FSQL_INCLUDE_PATH.'/extensions');

require FSQL_INCLUDE_PATH.'/fSQLCursors.php';
require FSQL_INCLUDE_PATH.'/fSQLParser.php';
require FSQL_INCLUDE_PATH.'/fSQLQuery.php';
require FSQL_INCLUDE_PATH.'/fSQLUtilities.php';
require FSQL_INCLUDE_PATH.'/drivers/fSQLBaseDriver.php';
require FSQL_INCLUDE_PATH.'/drivers/fSQLMemoryDriver.php';
require FSQL_INCLUDE_PATH.'/drivers/fSQLMasterDriver.php';

/**
 * This class provides a working environment for fSQL mechanisms. Class contains
 * required information such as databases and registered functions. 
 * @package fsql
 * @subpackage classes
 */
class fSQLEnvironment
{
	/**
	* @var array
	*/
	var $updatedTables = array();
	/**
	* @var array
	*/
	var $lockedTables = array();
	/**
	* @var array
	*/
	var $databases = array();
	/**
	* Pointer(?) to currently using database
	* @var ???
	*/
	var $currentDB = null;
	/**
	* Pointer(?) to currently using schema
	* @var ???
	*/
	var $currentSchema = null;
	/**
	* Last error message issued by fSQL
	* @var string
	*/
	var $error_msg = null;
	/**
	* Number of total queries run.
	* @var integer
	*/
	var $query_count = 0;
	/**
	* 
	* @var array
	*/
	var $join_lambdas = array();
	/**
	* 
	* @var integer
	*/
	var $affected = 0;
	/**
	* 
	* @var integer
	*/
	var $insert_id = 0;
	/**
	* 
	* @var bool
	*/
	var $auto = 1;
	/**
	* Array of registered (i.e available) functions
	* @var array
	*/
	var $registered_functions = array();
	/**
	* 
	* @var array
	*/
	var $resultSets = array();
	/**
	* 
	* @var ???
	*/
	var $parser;
	
	/**
	* This is a class constructor. Master database is created and inserted
	* into master schema. 
	*/ 
	function fSQLEnvironment()
	{
		// get current time in microseconds and feed srand method with
		// this value for future usage.
		list($usec, $sec) = explode(' ', microtime());
		srand((float) $sec + ((float) $usec * 100000));
		
		// create masterdatabase object; environment is current object
		// and name is FSQL
		$db =& new fSQLMasterDatabase($this, 'FSQL');
		$db->create();
		// insert the reference of masterdatabase to environment's 
		// database array with its name as index.
		$this->databases['FSQL'] =& $db;
		
		// insert masterdatabase to environment's master schema
		$master =& $this->_get_master_schema();
		$master->addDatabase($db);
		
		// create a new parser.
		$this->parser = new fSQLParser();
	}

	/**
	* Defines a database with given $name at the given $path. If $path is 
	* equals to FSQL_MEMORY_DB_PATH database is defined on memory not on the
	* file system. 
	* @param string $name 
	* @param string $path 
	*/
	function define_db($name, $path)
	{
		$this->error_msg = null;

		// if $path is equal to FSQL_MEMORY_DB_PATH database is created
		// on memory else fSQLStandardDriver is included and database is
		// created on filesystem.   		
		if($path !== FSQL_MEMORY_DB_PATH) {
			require FSQL_INCLUDE_PATH.'/drivers/fSQLStandardDriver.php';
			$db =& new fSQLDatabase($this, $name, $path);
		}
		else {
			$db =& new fSQLMemoryDatabase($this, $name);
		} 
		
		// insert the reference of new database to databases array.
		$this->databases[$name] =& $db;

		// insert database reference to master schema if database is 
		// created succesfully else delete reference from databases array.
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
	
	/**
	* Defines a schema within given database name. This schema also inserted
	* environment object's master schema.
	* @param string $db_name 
	* @param string $schema_name
	* @return bool true if schema succesfully defined.
	*/
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

	/**
	* Selects database with name $name. Selection means, assigning currentDB
	* variable with database reference and currentSchema with database's 
	* public schema. Database is selected if it is defined previously.
	* @param string $name 
	* @return bool true if database is selected.
	*/
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

	/**
	* Selects schema of a database. Selection means; assigning currentDB
	* variable with database reference and currentSchema with database's 
	* schema whose name given as argument. 
	* @param string $db_name 
	* @param string $schema_name 
	* @return bool ???.
	*/
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
	
	/**
	* Closes all result sets and databases.  
	*/
	function close()
	{
		$this->_unlock_tables();
		
		// close all resulsets.
		foreach(array_keys($this->resultSets) as $rs_id)
			$this->resultSets[$rs_id]->close();
		
		// close all databases.
		foreach (array_keys($this->databases) as $db_name)
			$this->databases[$db_name]->close();

		// reset variables.
		$this->resultSets = array();
		$this->databases = array();
		$this->updatedTables = array();
		$this->join_lambdas = array();
		$this->databases = array();
		$this->currentDB = null;
		$this->error_msg = null;
	}

	/**
	* Returns error message.  
	* @return string ???.
	*/
	function error()
	{
		return $this->error_msg;
	}

	/**
	* This method enables and disables MYSQL extension according to value 
	* of $enable and current parser type. If enable is true and parser is 
	* not mysql, a mysql parser is created. If enable is false and the parser 
	* is mysql sql parser is created.
	* @param bool $enable 
	*/
	function enable_mysql_exstensions($enable)
	{
		if($enable && !is_a($this->parser, 'fSQLParserMySQL'))
		{
			// include mysql extension if not exists.
			if(!class_exists('fSQLParserMySQL'))
				require FSQL_EXTENSIONS_PATH.'/mysql/fSQLParserMySQL.php';
			$this->parser = new fSQLParserMySQL($this);
		}
		else if(!$enable && is_a($this->parser, 'fSQLParserMySQL'))
		{
			$this->parser = new fSQLParser($this);
		}
	}

	/**
	* Registers php equivalent of a given sql function. 
	* @param string $sqlName
	* @param string $phpName
	* @return bool always true 
	*/
	function register_function($sqlName, $phpName)
	{
		$this->registered_functions[$sqlName] = $phpName;
		return true;
	}

	/**
	* Sets error message. 
	* @param string $error error message
	* @return bool always false
	*/
	function _set_error($error)
	{
		$this->error_msg = $error."\r\n";
		return false;
	}

	/**
	* Wrapper method calling _set_error with a formatted message.   
	* @param string $db_name
	* @param string $schema_name
	*/
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

	/**
	* Wrapper method calling _set_error with a formatted message.   
	* @param string $table_name_pieces
	*/
	function _error_table_not_exists($table_name_pieces)
	{
		$table_name = $this->_build_table_name($table_name_pieces);
		return $this->_set_error("Table {$table_name} does not exist"); 
	}

	/**
	* Wrapper method calling _set_error with a formatted message.   
	* @param string $table_name_pieces
	*/
	function _error_table_read_lock($table_name_pieces)
	{
		$table_name = $this->_build_table_name($table_name_pieces);
		return $this->_set_error("Table {$table_name} is locked for reading only"); 
	}
	
	function escape_string($string)
	{
		return str_replace(array('\\', '\0', '\n', '\r', '\t', '\''), array('\\\\', '\\0', '\\n', '\\', '\\t', '\\\''), $string);
	}
	
	/**
	* Returns number of affected rows.   
	* @return integer ???
	*/
	function affected_rows()
	{
		return $this->affected;
	}

	/**
	* Returns last inserted id ???.   
	* @return integer ???
	*/
	function insert_id()
	{
		return $this->insert_id;
	}
	
	/**
	* Returns an array of all databases where database names are indexes and 
	* paths are value.    
	* @return array database path array
	*/
	function list_dbs()
	{
		$databases = array();
		foreach($this->databases as $db_name => $db)
		{
			$databases[$db_name] = $db->getPath();
		}
		return $databases;
	}

	/**
	* Returns query count.   
	* @return integer
	*/
	function query_count()
	{
		return $this->query_count;
	}
	
	/**
	* This method returs master schema of the master database.  
	* @return fSQLStandardSchema
	*/
	function &_get_master_schema()
	{
		return $this->databases['FSQL']->getSchema('master');
	}
	
	/**
	* Returns database $db_name. If $db_name argumen is not given, method
	* returns currentDB.   
	* @param string $db_name
	* @return fSQLDatabase|bool
	*/
	function &_get_database($db_name)
	{
		$db = false;
		
		// If $db_name is given try to return database else (not given)
		// try to return currentDB. 
		if(!$db_name)
		{
			// if there is no selected database set error
			if($this->currentDB !== null)
				$db =& $this->currentDB;
			else
				$this->_set_error('No database specified');
		}
		else
		{
			// if database $db_name is not defined set error
			if(isset($this->databases[$db_name]))
				$db =& $this->databases[$db_name];
			else
				$this->_set_error("Database $db_name not found"); 
		}
		
		return $db;
	}
	
	/**
	* Finds and returns schema of a database.
	* returns currentDB.   
	* @param string $db_name
	* @param string $schema_name
	* @return fSQLStandardSchema|bool
	*/
	function &_find_schema($db_name, $schema_name)
	{
		$schema = false;
		
		$db =& $this->_get_database($db_name);
		if($db !== false)
		{
			// if $schema_name is not given try to find current schema
			// else ( $schema_name is given) try to find schema.
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
	
	/**
	* Finds and returns a table.
	* @param string $name_pieces
	* @return fSQLTable|bool
	*/
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
	
	/**
	* Unlocks all locked tables in the environment 
	* @see fSQLStandardTable::unlock
	*/
	function _unlock_tables()
	{
		// unlock all locked tables.
		foreach (array_keys($this->lockedTables) as $index )
			$this->lockedTables[$index]->unlock();
		// reset locked tables array.
		$this->lockedTables = array();
	}

	/**
	* ???
	*/
	function _begin()
	{
		$this->auto = 0;
		$this->_unlock_tables();
		$this->_commit();
	}
	
	/**
	* Commits all updated tables.
	* @see fSQLStandardTable::commit
	*/
	function _commit()
	{
		$this->auto = 1;
		// commit all updated tables
		foreach (array_keys($this->updatedTables) as $index ) {
			$this->updatedTables[$index]->commit();
		}
		// reset updatedTables array
		$this->updatedTables = array();
	}

	/**
	* Rollbacks all updated tables.
	* @see fSQLStandardTable::rollback
	*/
	function _rollback()
	{
		$this->auto = 1;
		// rollback all updated tables
		foreach (array_keys($this->updatedTables) as $index ) {
			$this->updatedTables[$index]->rollback();
		}
		// reset updatedTables array
		$this->updatedTables = array();
	}
	
	/**
	* Parses, prepares and executes given query. 
	* @param string query
	* @return bool 
	*/
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

	/**
	* Prepares a value to sql insert statement (i.e if value is null it is 
	* replaced with NULL, if value is string it is surronded with "''").  
	* @param string|integer|bool value
	* @return string|integer|bool modified value
	*/
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
	
	/**
	* Parsing will be changed.
	*/
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

	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function _create_result_set($columns, $entries)
	{
		$rs_id = !empty($this->resultSets) ? max(array_keys($this->resultSets)) + 1 : 1;
		$this->resultSets[$rs_id] =& new fSQLResultSet($columns, $entries);
		return $rs_id;
	}

	/**
	* Returns result set whose id is $rs_id. If resultset with given id is
	* not defined, false is returned. 
	* @param integer rs_id
	* @return bool|fSQLResultSet
	*/
	function &get_result_set($rs_id)
	{
		$rs = $this->_is_valid_result_set($rs_id) ? $this->resultSets[$rs_id] : false;
		return $rs;
	}
	
	/**
	* Returns whether resultset with given id is valid.
	* @param integer rs_id
	* @return bool
	*/
	function _is_valid_result_set($rs_id) {
		return $rs_id !== false && isset($this->resultSets[$rs_id]->columns);
	}
	
	/**
	* Returns results whose id is $rs_id. Result structure is depends on 
	* type argument. Possible types are: FSQL_NUM, FSQL_ASSOC, FSQL_BOTH. If
	* type is FSQL_NUM resultset data(???) is returned. If type is FSQL_ASSOC
	* an array having the form "columnname => entry" is returned. If type is
	* FSQL_BOTH ???
	* @param integer rs_id
	* @param integer type
	* @return array|bool
	*/
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
	
	/**
	* Returns a single result row of result set whose id is rs_id. Result 
	* structure is depends on type argument. Possible types are: FSQL_NUM, 
	* FSQL_ASSOC, FSQL_BOTH. ???
	* @param integer rs_id
	* @param integer type
	* @return array|bool
	*/
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
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function fetch_assoc($results) { return $this->fetch_array($results, FSQL_ASSOC); }

	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function fetch_row($results) { return $this->fetch_array($results, FSQL_NUM); }

	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function fetch_both($results) { return $this->fetch_array($results, FSQL_BOTH); }
 
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
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
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function data_seek($rs_id, $i)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->dataCursor->seek($i);
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function num_rows($rs_id)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->dataCursor->numRows();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function num_fields($rs_id)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->columnsCursor->numRows();
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
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
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function field_seek($rs_id, $i)
	{
		if($this->_is_valid_result_set($rs_id)) {
			return $this->resultSets[$rs_id]->columnsCursor->seek($i);
		} else {
			return $this->_set_error('Bad results id passed in');
		}
	}

	/**
	*
	* @param 
	* @param 
	* @return 
	*/
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

	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function _fsql_not($x)
	{
		$c = ~$x & 3;
		return (($c << 1) ^ ($c >> 1)) & 3;
	}

	/**
	*
	* @param 
	* @param 
	* @return 
	*/
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
	
	/**
	* Checks if a value exists in an array.
	* @param mixed needle
	* @param mixed haystack
	* @return bool
	*/
	function _fsql_in($needle, $haystack)
	{
		if($needle !== null)
		{
			return (in_array($needle, $haystack)) ? FSQL_TRUE : FSQL_FALSE;
		}
		else
			return FSQL_UNKNOWN;
	}
	
	/**
	*
	* @param 
	* @param 
	* @return 
	*/
	function _fsql_regexp($left, $right)
	{
		if($left !== null && $right !== null)
			return (eregi($right, $left)) ? FSQL_TRUE : FSQL_FALSE;
		else
			return FSQL_UNKNOWN;
	}

	/**
	* Method converts typecode to  name. 
	* @param string type
	* @return string name 
	*/
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
