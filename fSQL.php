<?php

define("FSQL_ASSOC",1,TRUE);
define("FSQL_NUM",  2,TRUE);
define("FSQL_BOTH", 3,TRUE);

define("FSQL_EXTENSION", ".cgi",TRUE);

define('FSQL_TYPE_DATE','d',true);
define('FSQL_TYPE_DATETIME','dt',true);
define('FSQL_TYPE_ENUM','e',true);
define('FSQL_TYPE_FLOAT','f',true);
define('FSQL_TYPE_INTEGER','i',true);
define('FSQL_TYPE_STRING','s',true);
define('FSQL_TYPE_TIME','t',true);

define('FSQL_WHERE_NORMAL',2,true);
define('FSQL_WHERE_ON',4,true);

define('FSQL_TRUE', 3, true);
define('FSQL_FALSE', 0,true);
define('FSQL_NULL', 1,true);
define('FSQL_UNKNOWN', 1,true);

// This function is in PHP5 but nowhere else so we're making it in case we're on PHP4
if (!function_exists('array_combine')) {
	function array_combine($keys, $values) {
		if(is_array($keys) && is_array($values) && count($keys) == count($values)) {
			$combined = array();
			foreach($keys as $indexnum => $key)
				$combined[$key] = $values[$indexnum];
			return $combined;
		}
		return false;
	}
}

/* A reentrant read write lock for a file */
class fSQLFileLock
{
	var $handle;
	var $filepath;
	var $lock;
	var $rcount = 0;
	var $wcount = 0;

	function fSQLFileLock($filepath)
	{
		$this->filepath = $filepath;
		$this->handle = null;
		$this->lock = 0;
	}

	function getHandle()
	{
		return $this->handle;
	}

	function acquireRead()
	{
		if($this->lock !== 0 && $this->handle !== null) {  /* Already have at least a read lock */
			$this->rcount++;
			return true;
		}
		else if($this->lock === 0 && $this->handle === null) /* New lock */
		{
			$this->handle = fopen($this->filepath, 'rb');
			if($this->handle)
			{
				flock($this->handle, LOCK_SH);
				$this->lock = 1;
				$this->rcount = 1;
				return true;
			}
		}

		return false;
	}

	function acquireWrite()
	{
		if($this->lock === 2 && $this->handle !== null)  /* Already have a write lock */
		{
			$this->wcount++;
			return true;
		}
		else if($this->lock === 1 && $this->handle !== null)  /* Upgrade a lock*/
		{
			flock($this->handle, LOCK_EX);
			$this->lock = 2;
			$this->wcount++;
			return true;
		}
		else if($this->lock === 0 && $this->handle === null) /* New lock */
		{
			touch($this->filepath); // make sure it exists
			$this->handle = fopen($this->filepath, 'r+b');
			if($this->handle)
			{
				flock($this->handle, LOCK_EX);
				$this->lock = 2;
				$this->wcount = 1;
				return true;
			}
		}

		return false;
	}

	function releaseRead()
	{
		if($this->lock !== 0 && $this->handle !== null)
		{
			$this->rcount--;

			if($this->lock === 1 && $this->rcount === 0) /* Read lock now empty */
			{
				// no readers or writers left, release lock
				flock($this->handle, LOCK_UN);
				fclose($this->handle);
				$this->handle = null;
				$this->lock = 0;
			}
		}

		return true;
	}

	function releaseWrite()
	{
		if($this->lock !== 0 && $this->handle !== null)
		{
			if($this->lock === 2) /* Write lock */
			{
				$this->wcount--;
				if($this->wcount === 0) // no writers left.
				{
					if($this->rcount > 0)  // only readers left.  downgrade lock.
					{
						flock($this->handle, LOCK_SH);
						$this->lock = 1;
					}
					else // no readers or writers left, release lock
					{
						flock($this->handle, LOCK_UN);
						fclose($this->handle);
						$this->handle = null;
						$this->lock = 0;
					}
				}
			}
		}

		return true;
	}
}

class fSQLTableCursor
{
	var $entries;
	var $num_rows;
	var $pos;

	function first()
	{
		$this->pos = 0;
	}

	function getRow()
	{
		if($this->pos >= 0 && $this->pos < $this->num_rows)
			return $this->entries[$this->pos];
		else
			return null;
	}

	function isDone()
	{
		return $this->pos < 0 || $this->pos >= $this->num_rows;
	}

	function last()
	{
		$this->pos = $this->num_rows - 1;
	}

	function previous()
	{
		$this->pos++;
	}

	function next()
	{
		$this->pos++;
	}

	function seek($pos)
	{
		if($pos >=0 & $pos < $count($this->entries))
			$this->pos = $pos;
	}
}

class fSQLTable
{
	var $cursor = NULL;
	var $columns = NULL;
	var $entries = NULL;
	var $data_load =0;
	var $uncommited = false;

	function &create($path_to_db, $table_name, $columnDefs)
	{
		$table =& new fSQLTable;
		$table->columns = $columnDefs;
		$table->temporary = true;
		return $table;
	}

	function exists() {
		return TRUE;
	}

	function getColumnNames() {
		return array_keys($this->getColumns());
	}

	function getColumns() {
		return $this->columns;
	}

	function setColumns($columns) {
		$this->columns = $columns;
	}

	function getEntries()
	{
		return $this->entries;
	}

	function &getCursor()
	{
		if($this->cursor == NULL)
			$this->cursor =& new fSQLTableCursor;

		$this->cursor->entries =& $this->entries;
		$this->cursor->num_rows = count($this->entries);
		$this->cursor->pos = 0;

		return $this->cursor;
	}

	function newCursor()
	{
		$cursor =& new fSQLTableCursor;
		$cursor->entries =& $this->entries;
		$cursor->num_rows = count($this->entries);
		$cursor->pos = 0;

		return $cursor;
	}

	function insertRow($data) {
		$this->entries[] = $data;
		$this->uncommited = true;
	}

	function updateRow($row, $data) {
		foreach($data as $key=> $value)
			$this->entries[$row][$key] = $value;
		$this->uncommited = true;
	}

	function deleteRow($row) {
		unset($this->entries[$row]);
		$this->uncommited = true;
	}

	function commit()
	{
		$this->uncommited = false;
	}

	function rollback()
	{
		$this->data_load = 0;
		$this->uncommited = false;
	}

	/* Unecessary for temporary tables */
	function readLock() { return true; }
	function writeLock() { return true; }
	function unlock() { return true; }
}

class fSQLCachedTable
{
	var $cursor = NULL;
	var $columns = NULL;
	var $entries = NULL;
	var $columns_path = NULL;
	var $data_path = NULL;
	var $columns_load = NULL;
	var $data_load = NULL;
	var $uncommited = false;
	var $columnsLockFile = NULL;
	var $columnsFile = NULL;
	var $dataLockFile = NULL;
	var $dataFile = NULL;
	var $lock = NULL;

	function fSQLCachedTable($path_to_db, $table_name)
	{
		$this->columns_path = $path_to_db.$table_name.'.columns';
		$this->data_path = $path_to_db.$table_name.'.data';
		$this->columnsLockFile = new fSQLFileLock($this->columns_path.'.lock.cgi');
		$this->columnsFile = new fSQLFileLock($this->columns_path.'.cgi');
		$this->dataLockFile = new fSQLFileLock($this->data_path.'.lock.cgi');
		$this->dataFile = new fSQLFileLock($this->data_path.'.cgi');
		$this->temporary = false;
	}

	function &create($path_to_db, $table_name, $columnDefs)
	{
		$table =& new fSQLCachedTable($path_to_db, $table_name);
		$table->columns = $columnDefs;

		list($msec, $sec) = explode(' ', microtime());
		$table->columns_load = $table->data_load = $sec.$msec;

		// create the columns lock
		$table->columnsLockFile->acquireWrite();
		$columnsLock = $table->columnsLockFile->getHandle();
		ftruncate($columnsLock, 0);
		fwrite($columnsLock, $table->columns_load);

		// create the columns file
		$table->columnsFile->acquireWrite();
		$toprint = $table->_printColumns($columnDefs);
		fwrite($table->columnsFile->getHandle(), $toprint);

		$table->columnsFile->releaseWrite();
		$table->columnsLockFile->releaseWrite();

		// create the data lock
		$table->dataLockFile->acquireWrite();
		$dataLock = $table->dataLockFile->getHandle();
		ftruncate($dataLock, 0);
		fwrite($dataLock, $table->data_load);

		// create the data file
		$table->dataFile->acquireWrite();
		fwrite($table->dataFile->getHandle(), "0\r\n");

		$table->dataFile->releaseWrite();
		$table->dataLockFile->releaseWrite();

		return $table;
	}

	function _printColumns($columnDefs)
	{
		$toprint = count($columnDefs)."\r\n";
		foreach($columnDefs as $name => $column) {
			if(!is_array($column['restraint'])) {
				$toprint .= $name.": ".$column['type'].";;".$column['auto'].";".$column['default'].";".$column['key'].";".$column['null'].";\r\n";
			} else {
				$toprint .= $name.": ".$column['type'].";".implode(",", $column['restraint']).";".$column['auto'].";".$column['default'].";".$column['key'].";".$column['null'].";\r\n";
			}
		}
		return $toprint;
	}

	function exists() {
		return file_exists($this->columns_path.'.cgi');
	}

	function getColumnNames() {
		return array_keys($this->getColumns());
	}

	function getColumns() {
		$this->columnsLockFile->acquireRead();
		$lock = $this->columnsLockFile->getHandle();

		$modified = fread($lock, 20);
		if($this->columns_load === NULL || strcmp($this->columns_load, $modified) < 0)
		{
			$this->columns_load = $modified;

			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();

			$line = fgets($columnsHandle);
			if(!preg_match("/^(\d+)/", $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return NULL;
			}

			$num_columns = $matches[1];

			for($i = 0; $i < $num_columns; $i++) {
				$line =	fgets($columnsHandle, 4096);
				if(preg_match("/(\S+): (dt|d|i|f|s|t|e);(.*);(0|1);(-?\d+(?:\.\d+)?|'.*'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
					$type = $matches[2];
					$default = $matches[5];
					if($type === 'i')
						$default = (int) $default;
					else if($type === 'f')
						$default = (float) $default;
					$this->columns[$matches[1]] = array(
						'type' => $type, 'auto' => $matches[4], 'default' => $default, 'key' => $matches[6], 'null' => $matches[7]
					);
					if(preg_match_all("/'(.*?(?<!\\\\))'/", $matches[3], $restraint) !== false) {
						$this->columns[$matches[1]]['restraint'] = $restraint[1];
					} else {
						$this->columns[$matches[1]]['restraint'] = array();
					}
				} else {
					return NULL;
				}
			}

			$this->columnsFile->releaseRead();
		}

		$this->columnsLockFile->releaseRead();

		return $this->columns;
	}

	function getEntries()
	{
		$this->_loadEntries();
		return $this->entries;
	}

	function &getCursor()
	{
		$this->_loadEntries();

		if($this->cursor == NULL)
			$this->cursor = new fSQLTableCursor;

		$this->cursor->entries =& $this->entries;
		$this->cursor->num_rows = count($this->entries);
		$this->cursor->pos = 0;

		return $this->cursor;
	}

	function newCursor()
	{
		$this->_loadEntries();

		$cursor =& new fSQLTableCursor;
		$cursor->entries =& $this->entries;
		$cursor->num_rows = count($this->entries);
		$cursor->pos = 0;

		return $cursor;
	}

	function _loadEntries()
	{
		$this->dataLockFile->acquireRead();
		$lock = $this->dataLockFile->getHandle();

		$modified = fread($lock, 20);
		if($this->data_load === NULL || strcmp($this->data_load, $modified) < 0)
		{
			$entries = NULL;
			$this->data_load = $modified;

			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();

			$line = fgets($dataHandle);
			if(!preg_match("/^(\d+)/", $line, $matches))
			{
				$this->dataFile->releaseRead();
				$this->dataLockFile->releaseRead();
				return NULL;
			}

			$num_entries = rtrim($matches[1]);

			if($num_entries != 0)
			{
				$skip = false;
				$entries = array();

				$columnDefs = array_values($this->getColumns());
				for($i = 0;  $i < $num_entries; $i++) {
					$line = rtrim(fgets($dataHandle, 4096));

					if(!$skip) {
						if(preg_match("/^(\d+):(.*)$/", $line, $matches))
						{
							$row = $matches[1];
							$data = trim($matches[2]);
						}
						else
							continue;
					}
					else {
						$data .= $line;
					}

					if(!preg_match("/(-?\d+(?:\.\d+)?|'.*?(?<!\\\\)'|NULL);$/", $line)) {
						$skip = true;
						continue;
					} else {
						$skip = false;
					}

					preg_match_all("#((-?\d+(?:\.\d+)?)|'.*?(?<!\\\\)'|NULL);#s", $data, $matches);
					for($m = 0; $m < count($matches[0]); $m++) {
						if($matches[1][$m] === 'NULL') {
							$entries[$row][$m] = NULL;
						} else if(!empty($matches[2][$m])) {
							$number = $matches[2][$m];
							if($strpos($number, '.') !== false) {
								$number = (float) $number;
							} else {
								$number = (int) $number;
							}
							$entries[$row][$m] = $number;
						} else if($columnDefs['type'] === FSQL_TYPE_ENUM) {
							$index = (int) $matches[2][$m];
							$entries[$row][$m] = $index > 0 ? $columnDefs[$m]['restraint'][$index] : "";
						} else {
							$entries[$row][$m] = $matches[1][$m];
						}
					}
				}
			}

			$this->entries = $entries;

			$this->dataFile->releaseRead();
		}

		$this->dataLockFile->releaseRead();

		return true;
	}

	function insertRow($data) {
		$this->_loadEntries();
		$this->entries[] = $data;
		$this->uncommited = true;
	}

	function updateRow($row, $data) {
		$this->_loadEntries();
		foreach($data as $key=> $value)
			$this->entries[$row][$key] = $value;
		$this->uncommited = true;
	}

	function deleteRow($row) {
		$this->_loadEntries();
		unset($this->entries[$row]);
		$this->uncommited = true;
	}

	function setColumns($columnDefs)
	{
		$this->columnsLockFile->acquireWrite();
		$lock = $this->columnsLockFile->getHandle();
		$modified = fread($lock, 20);

		$this->columns = $columnDefs;

		list($msec, $sec) = explode(' ', microtime());
		$this->columns_load = $sec.$msec;
		fseek($lock, 0, SEEK_SET);
		fwrite($lock, $this->columns_load);

		$this->columnsFile->acquireWrite();

		$toprint = $this->_printColumns($columnDefs);
		$columnsHandle = $this->columnsFile->getHandle();
		ftruncate($columnsHandle, 0);
		fwrite($columnsHandle, $toprint);

		$this->columnsFile->releaseWrite();
		$this->columnsLockFile->releaseWrite();
	}

	function commit()
	{
		if($this->uncommited === false)
			return;

		$this->dataLockFile->acquireWrite();
		$lock = $this->dataLockFile->getHandle();
		$modified = fread($lock, 20);

		if($this->data_load === NULL || strcmp($this->data_load, $modified) >= 0)
		{
			$columnDefs = array_values($this->getColumns());
			$toprint = count($this->entries)."\r\n";
			foreach($this->entries as $number => $entry) {
				$toprint .= $number.': ';
				foreach($entry as $key => $value) {
					if($value === NULL) {
						$value = 'NULL';
					} else if($columnDefs[$key]['type'] === FSQL_TYPE_ENUM) {
						$value = (int) array_search($value, $columnDefs[$key]['restraint']);;
					} else if(is_string($value)) {
						$value = "'$value'";
					}
					$toprint .= $value.';';
				}
				$toprint .= "\r\n";
			}
		} else {
			$toprint = "0\r\n";
		}

		list($msec, $sec) = explode(' ', microtime());
		$this->data_load = $sec.$msec;
		fseek($lock, 0, SEEK_SET);
		fwrite($lock, $this->data_load);

		$this->dataFile->acquireWrite();

		$dataHandle = $this->dataFile->getHandle();
		ftruncate($dataHandle, 0);
		fwrite($dataHandle, $toprint);

		$this->dataFile->releaseWrite();
		$this->dataLockFile->releaseWrite();

		$this->uncommited = false;
	}

	function rollback()
	{
		$this->data_load = 0;
		$this->uncommited = false;
	}

	function isReadLocked()
	{
		return $this->lock === 'r';
	}

	function readLock()
	{
		$success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
	 		&& $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
		if($success) {
			$this->lock = 'r';
			return true;
		} else {
			$this->unlock();  // release any locks that did work if at least one failed
			return false;
		}
	}

	function writeLock()
	{
		$success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
			&& $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
		if($success) {
			$this->lock = 'w';
			return true;
		} else {
			$this->unlock();  // release any locks that did work if at least one failed
			return false;
		}
	}

	function unlock()
	{
		if($this->lock === 'r')
		{
			$this->columnsLockFile->releaseRead();
			$this->columnsFile->releaseRead();
			$this->dataLockFile->releaseRead();
			$this->dataFile->releaseRead();
		}
		else if($this->lock === 'w')
		{
			$this->columnsLockFile->releaseWrite();
			$this->columnsFile->releaseWrite();
			$this->dataLockFile->releaseWrite();
			$this->dataFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

class fSQLDatabase
{
	var $name = NULL;
	var $path_to_db = NULL;
	var $loadedTables = array();

	function close()
	{
		unset($this->name, $this->path_to_db, $this->loadedTables);
	}

	function createTable($table_name, $columns, $temporary = false)
	{
		$table = NULL;

		if(!$temporary) {
			$table = fSQLCachedTable::create($this->path_to_db, $table_name, $columns);
		} else {
			$table = fSQLTable::create($this->path_to_db, $table_name, $columns);
			$this->loadedTables[$table_name] =& $table;
		}

		return $table;
	}

	function &getTable($table_name)
	{
		if(!isset($this->loadedTables[$table_name])) {
			$table = new fSQLCachedTable($this->path_to_db, $table_name);
			$this->loadedTables[$table_name] = $table;
			unset($table);
		}

		return $this->loadedTables[$table_name];
	}

	function listTables()
	{
		$dir = opendir($this->path_to_db);

		$tables = array();
		while (false !== ($file = readdir($dir))) {
			if ($file != '.' && $file != '..' && !is_dir($file)) {
				if(substr($file, -12) == '.columns.cgi') {
					$tables[] = substr($file, 0, -12);
				}
			}
		}

		closedir($dir);

		return $tables;
	}

	function &loadTable($table_name)
	{
		$table =& $this->getTable($table_name);
		if(!$table->exists())
			return NULL;

		$table->_loadEntries();

		$old_style_table = array('columns' => $table->getColumns(), 'entries' => $table->entries);
		return $old_style_table;
	}

	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		$oldTable =& $this->getTable($old_table_name);
		if($oldTable->exists()) {
			if(!$oldTable->temporary) {
				$newTable = $new_db->createTable($new_table_name,  $oldTable->getColumns());
				copy($oldTable->data_path.'.cgi', $newTable->data_path.'.cgi');
				copy($oldTable->data_path.'.lock.cgi', $newTable->data_path.'.lock.cgi');
				$this->dropTable($old_table_name);
			} else {
				$new_db->loadedTables[$new_table_name] =& $this->loadedTables[$old_table_name];
				unset($this->loadedTables[$old_table_name]);
			}

			return true;
		} else {
			return false;
		}
	}

	function dropTable($table_name)
	{
		$table =& $this->getTable($table_name);
		if($table->exists()) {
			if(!$table->temporary) {
				unlink($table->columns_path.'.cgi');
				unlink($table->columns_path.'.lock.cgi');
				unlink($table->data_path.'.cgi');
				unlink($table->data_path.'.lock.cgi');
			}

			$table = NULL;
			unset($this->loadedTables[$table_name]);
			unset($table);

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

class fSQLEnvironment
{
	var $updatedTables = array();
	var $lockedTables = array();
	var $databases = array();
	var $currentDB = NULL;
	var $error_msg = NULL;
	var $query_count = 0;
	var $cursors = array();
	var $data = array();
	var $join_lambdas = array();
	var $affected = 0;
	var $insert_id = 0;
	var $auto = 1;

	var $allow_func = array('abs','acos','asin','atan2','atan','ceil','cos','crc32','exp','floor',
	   'ltrim','md5','pi','pow','rand','rtrim','round','sha1','sin','soundex','sqrt','strcmp','tan');
	var $custom_func = array('concat','concat_ws','count','curdate','curtime','database','dayofweek',
	   'dayofyear','elt','from_unixtime','last_insert_id', 'left','locate','log','log2','log10','lpad','max','min',
	   'mod','month','now','repeat','right','row_count','sign','substring_index','sum','truncate','unix_timestamp',
	   'weekday','year');
	var $renamed_func = array('conv'=>'base_convert','ceiling' => 'ceil','degrees'=>'rad2deg','format'=>'number_format',
	   'length'=>'strlen','lower'=>'strtolower','ln'=>'log','power'=>'pow','quote'=>'addslashes',
	   'radians'=>'deg2rad','repeat'=>'str_repeat','replace'=>'strtr','reverse'=>'strrev',
	   'rpad'=>'str_pad','sha' => 'sha1', 'substring'=>'substr','upper'=>'strtoupper');

	function define_db($name, $path)
	{
		$path = realpath($path);
		if($path === FALSE || !is_dir($path)) {
			if(@mkdir($path, 0777))
				$path = realpath($path);
		} else if(!is_readable($path) || !is_writeable($path)) {
			chmod($path, 0777);
		}

		if($path && substr($path, -1) != '/')
			$path .= '/';

		list($usec, $sec) = explode(' ', microtime());
		srand((float) $sec + ((float) $usec * 100000));

		if(is_dir($path) && is_readable($path) && is_writeable($path)) {
			$db = new fSQLDatabase;
			$db->name = $name;
			$db->path_to_db = $path;
			$this->databases[$name] =& $db;
			unset($db);
			return true;
		} else {
			$this->_set_error("Path to directory for {$name} database is not valid.  Please correct the path or create the directory and chmod it to 777.");
			return false;
		}
	}

	function select_db($name)
	{
		if(isset($this->databases[$name])) {
			$this->currentDB =& $this->databases[$name];
			$this->currentDB->name = $name;
			unset($name);
			return true;
		} else {
			$this->_set_error("No database called {$name} found");
			return false;
		}
	}

	function close()
	{
		foreach (array_keys($this->databases) as $db_name ) {
			$this->databases[$db_name]->close();
		}
		unset($this->Columns, $this->cursors, $this->data, $this->currentDB, $this->databases, $this->error_msg, $this->join_lambdas);
	}

	function error()
	{
		return $this->error_msg;
	}

	function register_function($sqlName, $phpName)
	{
		$this->renamed_func[$sqlName] = $phpName;
		return TRUE;
	}

	function _set_error($error)
	{
		$this->error_msg = $error."\r\n";
		return NULL;
	}

	function _error_table_not_exists($db_name, $table_name)
	{
		$this->error_msg = "Table {$db_name}.{$table_name} does not exist";
	}

	function _error_table_read_lock($db_name, $table_name)
	{
		$this->error_msg = "Table {$db_name}.{$table_name} is locked for reading only";
	}

	function &_load_table(&$db, $table_name)
	{
		$table =& $db->loadTable($table_name);
		if(!$table)
			$this->_set_error("Unable to load table {$db->name}.{$table_name}");

		return $table;
	}

	function escape_string($string)
	{
		return str_replace(array("\\", "\0", "\n", "\r", "\t", "'"), array("\\\\", "\\0", "\\n", "\\", "\\t", "\\'"), $string);
	}

	function affected_rows()
	{
		return $this->affected;
	}

	function insert_id()
	{
		return $this->insert_id;
	}

	function num_rows($id)
	{
		if(isset($this->data[$id])) {
			return count($this->data[$id]);
		} else {
			return 0;
		}
	}

	function query_count()
	{
		return $this->query_count;
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
		list($function, ) = explode(" ", $query);
		$this->query_count++;
		$this->error_msg = NULL;
		switch(strtoupper($function)) {
			case 'CREATE':		return $this->_query_create($query);
			case 'SELECT':		return $this->_query_select($query);
			//case "SEARCH":		return $this->_query_search($query);
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
			default:			$this->_set_error('Invalid Query');  return NULL;
		}
	}

	function _query_begin($query)
	{
		if(preg_match("/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is", $query, $matches)) {
			$this->_begin();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}

	function _query_start($query)
	{
		if(preg_match("/\ASTART\s+TRANSACTION\s*[;]?\Z/is", $query, $matches)) {
			$this->_begin();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}

	function _query_commit($query)
	{
		if(preg_match("/\ACOMMIT\s*[;]?\Z/is", $query, $matches)) {
			$this->_commit();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}

	function _query_rollback($query)
	{
		if(preg_match("/\AROLLBACK\s*[;]?\Z/is", $query, $matches)) {
			$this->_rollback();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}

	function _query_create($query)
	{
		if(preg_match("/\ACREATE(?:\s+(TEMPORARY))?\s+TABLE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*?)`?(?:\s*\((.+)\)|\s+LIKE\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*))/is", $query, $matches)) {

			list(, $temporary, $ifnotexists, $db_name, $table_name, $column_list) = $matches;

			if(!$table_name) {
				$this->_set_error("No table name specified");
				return NULL;
			}

			if(!$db_name)
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$db_name];

			$table =& $db->getTable($table_name);
			if($table->exists()) {
				if(empty($ifnotexists)) {
					$this->_set_error("Table {$db->name}.{$table_name} already exists");
					return NULL;
				} else {
					return true;
				}
			}

			$temporary = !empty($temporary) ? true : false;

			if(!isset($matches[6])) {
				//preg_match_all("/(?:(KEY|PRIMARY KEY|UNIQUE) (?:([A-Z][A-Z0-9\_]*)\s*)?\((.+?)\))|(?:`?([A-Z][A-Z0-9\_]*?)`?(?:\s+((?:TINY|MEDIUM|BIG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)(\s+UNSIGNED)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);
				preg_match_all("/(?:(?:CONSTRAINT\s+(?:`?[A-Z][A-Z0-9\_]*`?\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+`?([A-Z][A-Z0-9\_]*)`?)?\s*\(`?(.+?)`?\))|(?:`?([A-Z][A-Z0-9\_]*?)`?(?:\s+((?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)\s*(UNSIGNED\s+)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);

				if(!$Columns) {
					$this->_set_error("Parsing error in CREATE TABLE query");
					return NULL;
				}

				$new_columns = array();

				for($c = 0; $c < count($Columns[0]); $c++) {
					//$column = str_replace("\"", "'", $column);
					if($Columns[1][$c])
					{
						if(!$Columns[3][$c]) {
							$this->_set_error("Parse Error: Excepted column name in \"{$Columns[1][$c]}\"");
							return null;
						}
						
						$keytype = strtolower($Columns[1][$c]);
						if($keytype === "index")
							$keytype = "key";
						$keycolumns = explode(",", $Columns[3][$c]);
						foreach($keycolumns as $keycolumn)
						{
							$new_columns[trim($keycolumn)]['key'] = $keytype{0};
						}
					}
					else
					{
						$name = $Columns[4][$c];
						$type = $Columns[5][$c];
						$options =  $Columns[8][$c];
						
						if(isset($new_columns[$name])) {
							$this->_set_error("Column '{$name}' redefined");
							return NULL;
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
								default:
									break;
							}
						}

						if(preg_match("/not\s+null/i", $options))
							$null = 0;
						else
							$null = 1;

						if(preg_match("/AUTO_INCREMENT/i", $options))
							$auto = 1;
						else
							$auto = 0;

						if($type == 'e') {
							preg_match_all("/'.*?(?<!\\\\)'/", $Columns[6][$c], $values);
							$restraint = $values[0];
						} else {
							$restraint = NULL;
						}

						if(preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\2))/is", $options, $matches)) {
							$default = $matches[1];
							if(!$null && strcasecmp($default, "NULL")) {
								if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
									if($type == 'i')
										$default = intval($matches[2]);
									else if($type == 'f')
										$default = floatval($matches[2]);
									else if($type == 'e') {
										if(in_array($default, $restraint))
											$default = array_search($default, $restraint) + 1;
										else
											$default = 0;
									}
								} else {
									if($type == 'i')
										$default = intval($default);
									else if($type == 'f')
										$default = floatval($default);
									else if($type == 'e') {
										$default = intval($default);
										if($default < 0 || $default > count($restraint)) {
											$this->_set_error("Numeric ENUM value out of bounds");
											return NULL;
										}
									}
								}
							}
						} else if($type == 's')
							// The default for string types is the empty string
							$default = "''";
						else
							// The default for dates, times, and number types is 0
							$default = 0;

						if(preg_match("/(PRIMARY KEY|UNIQUE(?: KEY)?)/is", $options, $keymatches)) {
							$keytype = strtolower($keymatches[1]);
							$key = $keytype{0};
						}
						else {
							$key = "n";
						}

						$new_columns[$name] = array('type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint);
					}
				}
			} else {
				$src_db_name = $matches[6];
				$src_table_name = $matches[7];

				if(!$src_db_name)
					$src_db =& $this->currentDB;
				else
					$src_db =& $this->databases[$src_db_name];

				$src_table =& $src_db->getTable($src_table_name);
				if($src_table->exists()) {
					$new_columns = $src_table->getColumns();
				} else {
					$this->_set_error("Table {$src_db->name}.{$src_table_name} doesn't exist");
					return null;
				}
			}

			$db->createTable($table_name, $new_columns, $temporary);

			return true;
		} else {
			$this->_set_error('Invalid CREATE query');
			return false;
		}
	}

	function _query_insert($query)
	{
		$this->affected = 0;

		// All INSERT/REPLACE queries are the same until after the table name
		if(preg_match("/\A((INSERT|REPLACE)(?:\s+(IGNORE))?\s+INTO\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?)\s+(.+?)\s*[;]?\Z/is", $query, $matches)) {
			list(, $beginning, $command, $ignore, $db_name, $table_name, $the_rest) = $matches;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}

		// INSERT...SELECT
		if(preg_match("/^SELECT\s+.+/is", $the_rest)) {
			$id = $this->_query_select($the_rest);
			while($values = fsql_fetch_array($id)) {
				$this->query_count--;
				$this->_query_insert($beginning." VALUES('".join("', '", $values)."')");
			}
			fsql_free_result($id);
			unset ($id, $values);
			return TRUE;
		}

		if(!$db_name)
			$db =& $this->currentDB;
		else
			$db =& $this->databases[$db_name];

		$table =& $db->getTable($table_name);
		if(!$table->exists()) {
			$this->_error_table_not_exists($db->name, $table_name);
			return NULL;
		}
		elseif($table->isReadLocked()) {
			$this->_error_table_read_lock($db->name, $table_name);
			return null;
		}

		$tableColumns = $table->getColumns();
		$tableCursor =& $table->getCursor();

		$check_names = 1;
		$replace = !strcasecmp($command, 'REPLACE');

		// Column List present and VALUES list
		if(preg_match("/^\(`?(.+?)`?\)\s+VALUES\s*\((.+)\)/is", $the_rest, $matches)) {
			$Columns = preg_split("/`?\s*,\s*`?/s", $matches[1]);
			$get_data_from = $matches[2];
		}
		// VALUES list but no column list
		else if(preg_match("/^VALUES\s*\((.+)\)/is", $the_rest, $matches)) {
			$get_data_from = $matches[1];
			$Columns = $table->getColumnNames();
			$check_names = 0;
		}
		// SET syntax
		else if(preg_match("/^SET\s+(.+)/is", $the_rest, $matches)) {
			$SET = explode(",", $matches[1]);
			$Columns= array();
			$data_values = array();

			foreach($SET as $set) {
				list($column, $value) = explode("=", $set);
				$Columns[] = trim($column);
				$data_values[] = trim($value);
			}

			$get_data_from = implode(",", $data_values);
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}

		preg_match_all("/\s*(DEFAULT|AUTO|NULL|'.*?(?<!\\\\)'|(?:[\+\-]\s*)?\d+(?:\.\d+)?|[^$])\s*(?:$|,)/is", $get_data_from, $newData);
		$dataValues = $newData[1];

		if($check_names == 1) {
			$i = 0;
			$TableColumns = $table->getColumnNames();

			if(count($dataValues) != count($Columns)) {
				$this->_set_error("Number of inserted values and columns not equal");
				return null;
			}

			foreach($Columns as $col_name) {
				if(!in_array($col_name, $TableColumns)) {
					$this->_set_error("Invalid column name '{$col_name}' found");
					return NULL;
				} else {
					$Data[$col_name] = $dataValues[$i++];
				}
			}

			if(count($Columns) != count($TableColumns)) {
				foreach($TableColumns as $col_name) {
					if(!in_array($col_name, $Columns)) {
						$Data[$col_name] = "NULL";
					}
				}
			}
		}
		else
		{
			$countData = count($dataValues);
			$countColumns = count($Columns);

			if($countData < $countColumns) {
				$Data = array_combine($Columns, array_pad($dataValues, $countColumns, "NULL"));
			} else if($countData > $countColumns) {
				$this->_set_error("Trying to insert too many values");
				return NULL;
			} else {
				$Data = array_combine($Columns, $dataValues);
			}
		}

		$newentry = array();

		////Load Columns & Data for the Table
		$colIndex = 0;
		foreach($tableColumns as $col_name => $columnDef)  {

			unset($delete);

			$data = trim($Data[$col_name]);
			$data = strtr($data, array("$" => "\$", "\$" => "\\\$"));

			////Check for Auto_Increment
			if((empty($data) || !strcasecmp($data, "AUTO") || !strcasecmp($data, "NULL")) && $columnDef['auto'] == 1) {
				$tableCursor->last();
				$lastRow = $tableCursor->getRow();
				if($lastRow !== NULL)
					$this->insert_id = $lastRow[$colIndex] + 1;
				else
					$this->insert_id = 1;
				$newentry[$colIndex] = $this->insert_id;
			}
			///Check for NULL Values
			else if((!strcasecmp($data, "NULL") && !$columnDef['null']) || empty($data) || !strcasecmp($data, "DEFAULT")) {
				$newentry[$colIndex] = $columnDef['default'];
			} else {
				$data = $this->_parse_value($columnDef, $data);
				if($data === false)
					return null;
				$newentry[$colIndex] = $data;
			}

			////See if it is a PRIMARY KEY or UNIQUE
			if($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
				if($replace) {
					$delete = array();
					$tableCursor->first();
					$n = 0;
					while(!$tableCursor->isDone()) {
						$row = $tableCursor->getRow();
						if($row[$colIndex] == $newentry[$colIndex]) { $delete[] = $n; }
						$tableCursor->next();
						$n++;
					}
					if(!empty($delete)) {
						foreach($delete as $d) {
							$this->affected++;
							$table->deleteRow($d);
						}
					}
				} else {
					$tableCursor->first();
					while(!$tableCursor->isDone()) {
						$row = $tableCursor->getRow();
						if($row[$colIndex] == $newentry[$colIndex]) {
							if(empty($ignore)) {
								$this->_set_error("Duplicate value for unique column '{$col_name}'");
								return NULL;
							} else {
								return TRUE;
							}
						}
						$tableCursor->next();
					}
				}
			}

			$colIndex++;
		}

		$table->insertRow($newentry);

		if($this->auto)
			$table->commit();
		else if(!in_array($table, $this->updatedTables))
			$this->updatedTables[] =& $table;

		$this->affected++;

		return TRUE;
	}

	////Update data in the DB
	function _query_update($query) {
		$this->affected = 0;
		if(preg_match("/\AUPDATE(?:\s+(IGNORE))?\s+(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?\s*\Z/is", $query, $matches)) {
			$matches[4] = preg_replace("/(.+?)(\s+WHERE)(.*)/is", "\\1", $matches[4]);
			$ignore = !empty($matches[1]);
			$table_name = $matches[3];

			if(!$matches[2])
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$matches[2]];

			$table =& $db->getTable($table_name);
			if(!$table->exists()) {
				$this->_error_table_not_exists($db->name, $table_name);
				return NULL;
			}
			elseif($table->isReadLocked()) {
				$this->_error_table_read_lock($db->name, $table_name);
				return null;
			}

			$columns = $table->getColumns();
			$cursor =& $table->getCursor();
			$keyCursor = $table->newCursor();

			if(preg_match_all("/`?((?:\S+)`?\s*=\s*(?:'(?:.*?)'|\S+))`?\s*(?:,|\Z)/is", $matches[4], $sets)) {
				foreach($sets[1] as $set) {
					$s = preg_split("/`?\s*=\s*`?/", $set);
					$SET[] = $s;
					if(!isset($columns[$s[0]])) {
						$this->_set_error("Invalid column name '{$s[0]}' found");
						return null;
					}
				}
				unset($s);
			}
			else
				$SET[0] = preg_split("/\s*=\s*/", $matches[4]);

			$where = null;
			if(isset($matches[5]))
			{
				$where = $this->_load_where($matches[5], false);
				if(!$where) {
					$this->_set_error('Invalid/Unsupported WHERE clause');
					return null;
				}
			}

			$alter_columns = array();
			foreach($columns as $column => $columnDef) {
				if($columnDef['type'] == 'e')
					$alter_columns[] = $column;
			}

			$newentry = array();
			$affected = 0;
			$skip = false;
			for($e = 0; !$cursor->isDone(); $e++, $cursor->next()) {

				unset($entry);
				$entry = $cursor->getRow();
				foreach($alter_columns as $column) {
					if($columns[$column]['type'] == 'e') {
						$i = $entry[$column];
						$entry[$column] = ($i == 0) ? "''" : $columns[$column]['restraint'][$i - 1];
					}
				}

				if($where != null) {
					$proceed = "";
					for($i = 0; $i < count($where); $i++) {
						if($i > 0 && $where[$i - 1]["next"] == "AND")
							$proceed .= " && ".$this->_where_functions($where[$i], $entry, $table_name);
						else if($i > 0 && $where[$i - 1]["next"] == "OR")
							$proceed .= " || ".$this->_where_functions($where[$i], $entry, $table_name);
						else
							$proceed .= intval($this->_where_functions($where[$i], $entry, $table_name) == 1);
					}
					eval("\$cont = $proceed;");
					if(!$cont)
						continue;
				}

				foreach($SET as $set) {
					list($column, $value) = $set;

					$columnDef = $columns[$column];

					if(!$columnDef['null'] && $value == "NULL")
						$value = $columnDef['default'];
					else if(preg_match("/\A([A-Z][A-Z0-9\_]*)/i", $value))
						$value = $entry[$value];
					else if($columnDef['type'] == 'i')
						if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $sets))
							$value = (int) $sets[1];
						else
							$value = (int) $value;
					else if($columnDef['type'] == 'f')
						if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $sets))
							$value = (float) $sets[1];
						else
							$value = (float) $value;
					else if($columnDef['type'] == 'e')
						if(in_array($value, $columnDef['restraint'])) {
							$value = array_search($value, $columnDef['restraint']) + 1;
						} else if(is_numeric($value))  {
							$value = (int) $value;
							if($value < 0 || $value > count($columnDef['restraint'])) {
								$this->_set_error("Numeric ENUM value out of bounds");
								return NULL;
							}
						} else {
							$value = $columnDef['default'];
						}

					$newentry[$column] = $value;

					////See if it is a PRIMARY KEY or UNIQUE
					if($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
						$keyCursor->first();
						$c = 0;
						while(!$keyCursor->isDone()) {
							$row = $keyCursor->getRow();
							if($row[$column] == $newentry[$column] && $e != $c) {
								if(!$ignore) {
									$this->_set_error("Duplicate value for unique column '{$column}'");
									return NULL;
								} else {
									$skip = true;
									break;
								}
							}
							$keyCursor->next();
							$c++;
						}
					}
				}

				if(!$skip) {
					$table->updateRow($e, $newentry);
					$affected++;
				}
			}

			$this->affected = $affected;

			if($this->affected)
			{
				if($this->auto)
					$table->commit();
				else if(!in_array($table, $this->updatedTables))
					$this->updatedTables[] =& $table;
			}

			return TRUE;
		} else {
			$this->_set_error('Invalid UPDATE query');
			return NULL;
		}
	}

	/*
		MERGE INTO
		  table_dest d
		USING
		  table_source s
		  table_source s
		ON
		  (s.id = d.id)
		when	 matched then update set d.txt = s.txt
		when not matched then insert (id, txt) values (s.id, s.txt);
	*/
	function _query_merge($query)
	{
		if(preg_match("/\AMERGE\s+INTO\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?(?:\s+AS\s+`?([A-Z][A-Z0-9\_]*)`?)?\s+USING\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)(?:\s+AS\s+([A-Z][A-Z0-9\_]*))?\s+ON\s+(.+?)(?:\s+WHEN\s+MATCHED\s+THEN\s+(UPDATE .+?))?(?:\s+WHEN\s+NOT\s+MATCHED\s+THEN\s+(INSERT .+?))?/is", $query, $matches)) {
			list( , $dest_db_name, $dest_table, $dest_alias, $src_db_name, $src_table, $src_alias, $on_clause) = $matches;

			if(!$dest_db_name)
				$dest_db =& $this->currentDB;
			else
				$dest_db =& $this->databases[$dest_db_name];

			if(!$src_db_name)
				$src_db =& $this->currentDB;
			else
				$src_db =& $this->databases[$src_db_name];

			if(!($dest = $this->_load_table($dest_db, $dest_table))) {
				return NULL;
			}

			if(!($src = $this->_load_table($src_db, $src_table))) {
				return NULL;
			}

			if(preg_match("/(?:\()?(\S+)\s*=\s*(\S+)(?:\))?/", $on_clause, $on_pieces)) {

			} else {
				$this->_set_error('Invalid ON clause');
				return NULL;
			}

			$TABLES = explode(",", $matches[1]);
			foreach($TABLES as $table_name) {
				$table_name = trim($table_name);
				if(!$this->table_exists($table_name)) { $this->error_msg = "Table $table_name does not exist";  return NULL; }
				$table = $this->load_table($table_name);
				$tables[] = $table;
			}
			foreach($tables as $table) {
				if($table['columns'] != $tables[1]['columns']) { $this->error_msg = "Columns in the tables to be merged don't match"; return NULL; }
				foreach($table['entries'] as $tbl_entry) { $entries[] = $tbl_entry; }
			}
			$this->print_tbl($matches[2], $tables[1]['columns'], $entries);
			return TRUE;
		} else {
			$this->_set_error("Invalid MERGE query");
			return NULL;
		}
	}

	////Select data from the DB
	function _query_select($query)
	{
		$randval = rand();
		$e = 0;

		if(!preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*)\s*[;]?\s*\Z/is', $query, $matches)) {
			return $this->_set_error('Invalid SELECT query');
		}

		$distinct = !strncasecmp($matches[1], "DISTINCT", 8);
		$has_random = !empty(trim($matches[2]));
		$isTableless = true;

		$Columns = array();
		$the_rest = $matches[3];
		$stop = false;
		while(!$stop && preg_match("/((?:\A|\s*)((?:(?:-?\d+(?:\.\d+)?)|'.*?(?<!\\\\)'|(?:[^\W\d]\w*\s*\(.*?\))|(?:(?:(?:[^\W\d]\w*)\.)?(?:(?:[^\W\d]\w*)|\*)))(?:\s+(?:AS\s+)?[^\W\d]\w*)?)\s*)(?:\Z|(from|where|order\s+by|limit)|,)/is", $the_rest, $ColumnPiece))
		{
			$Columns[] = $ColumnPiece[2];
			$stop = !empty($ColumnPiece[3]);
			$idx = !$stop ? 0 : 1;
			$the_rest = substr($the_rest, strlen($ColumnPiece[$idx]));
		}

		$data = array();
		$joins = array();
		$joined_info = array( 'tables' => array(), 'offsets' => array(), 'columns' =>array() );
		if(preg_match('/\Afrom\s+(.+?)(\s+(?:where|order?\s+by|limit)\s+(?:.+))?\s*\Z/is', $the_rest, $from_matches))
		{
			$isTableless = false;
			$tables = array();

			if(isset($from_matches[2])) {
				$the_rest = $from_matches[2];
			}

			$tbls = explode(',', $from_matches[1]);
			foreach($tbls as $tbl) {
				if(preg_match('/\A\s*(`?(?:[^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s*(.*)/is', $tbl, $table_matches)) {
					list(, $db_name, $table_name, $saveas, $table_unparsed) = $table_matches;
					if(empty($db_name)) {
						$db_name = $this->currentDB->name;
					}
					if(empty($saveas)) {
						$saveas = $table_name;
					}

					$db =& $this->databases[$db_name];

					if(!($table =& $db->getTable($table_name))) {
						return NULL;
					}

					if(!isset($tables[$saveas])) {
						$tables[$saveas] =& $table;
					} else {
						return $this->_set_error("Table named '$saveas' already specified");
					}

					$joins[$saveas] = array('fullName' => array($db_name, $table_name), 'joined' => array());
					$table_columns = $table->getColumns();
					$join_columns_size = count($table_columns);
					$joined_info['tables'][$saveas] = $table_columns;
					$joined_info['offsets'][$saveas] = count($joined_info['columns']);
					$joined_info['columns'] = array_merge($joined_info['columns'], array_keys($table_columns));

					$join_data = $table->getEntries();

					if(!empty($table_unparsed)) {
						preg_match_all("/\s*(?:((?:LEFT|RIGHT|FULL)(?:\s+OUTER)?|INNER)\s+)?JOIN\s+(`?(?:[^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s+(USING|ON)\s*(?:(?:\((.*?)\))|(?:(?:\()?((?:\S+)\s*=\s*(?:\S+)(?:\))?)))/is", $table_unparsed, $join);
						$numJoins = count($join[0]);
						for($i = 0; $i < $numJoins; ++$i) {
							$join_name = trim($join[1][$i]);
							$join_db_name = $join[2][$i];
							if(empty($join_db_name)) {
								$join_db_name = $this->currentDB->name;
							}
							$join_table_name = $join[3][$i];
							$join_table_saveas = $join[4][$i];
							if(empty($join_table_saveas)) {
								$join_table_saveas = $join_table_name;
							}

							$join_db =& $this->databases[$join_db_name];

							if(!($join_table =& $join_db->getTable($join_table_name))) {
								return NULL;
							}

							if(!isset($tables[$join_table_saveas])) {
								$tables[$join_table_saveas] =& $join_table;
							} else {
								return $this->_set_error("Table named '$join_table_saveas' already specified");
							}

							$clause = $join[5][$i];
							if(!strcasecmp($clause, "ON")) {
								$conditions = isset($list[6][$i]) ? $join[6][$i] : $join[7][$i];
							}
							else if(!strcasecmp($clause, "USING")) {
								$shared_columns = preg_split('/\s*,\s*/', trim($join[6][$i]));

								$conditional = '';
								foreach($shared_columns as $shared_column) {
									$conditional .= " AND {{left}}.$shared_column=$join_table_alias.$shared_column";
								}
								$conditions = substr($conditional, 5);
							}

							$join_table_columns = $join_table->getColumns();
							$join_table_column_names = array_keys($join_table_columns);
							$joining_columns_size = count($join_table_column_names);

							$joined_info['tables'][$join_table_saveas] = $join_table_columns;
							$new_offset = count($joined_info['columns']);
							$joined_info['columns'] = array_merge($joined_info['columns'], $join_table_column_names);

							$conditional = $this->_build_where($conditions, $joined_info, FSQL_WHERE_ON);
							if(!$conditional) {
								return $this->_set_error('Invalid/Unsupported WHERE clause');
							}

							if(!isset($this->join_lambdas[$conditional])) {
								$join_function = create_function('$left_entry,$right_entry', "return $conditional;");
								$this->join_lambdas[$conditional] = $join_function;
							} else {
								$join_function = $this->join_lambdas[$conditional];
							}

							$joined_info['offsets'][$join_table_saveas] = $new_offset;
							$joins[$saveas]['joined'][] = array('alias' => $join_table_saveas, 'fullName' => array($join_db_name, $join_table_name), 'type' => $join_name, 'clause' => $clause, 'comparator' => $join_function);

							$joining_entries = $join_table->getEntries();
							if(!strncasecmp($join_name, "LEFT", 4)) {
								$join_data = $this->_left_join($join_data, $joining_entries, $join_function, $joining_columns_size);
							} else if(!strncasecmp($join_name, "RIGHT", 5)) {
								$join_data = $this->_right_join($join_data, $joining_entries, $join_function, $join_columns_size);
							} else if(!strncasecmp($join_name, "FULL", 4)) {
								$join_data = $this->_full_join($join_data, $joining_entries, $join_function, $join_columns_size, $joining_columns_size);
							} else {
								$join_data = $this->_inner_join($join_data, $joining_entries, $join_function);
							}

							$join_columns_size += $joining_columns_size;
						}
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
						} else {
							$data = $join_data;
						}
					}
				} else {
					return $this->_set_error('Invalid table list');
				}
			}
		}

		$this->tosort = array();
		$where = null;
		$limit = null;

		if(preg_match('/\s+WHERE\s+((?:.+?)(?:(?:(?:(?:\s+)(?:AND|OR)(?:\s+))?(?:.+?)?)*?)?)(\s+(?:HAVING|(?:GROUP|ORDER)\s+BY|LIMIT|FETCH).*)?\Z/is', $the_rest, $additional)) {
			$the_rest = isset($additional[2]) ? $additional[2] : '';
			$where = $this->_build_where($additional[1], $joined_info);
			if(!$where)
				return $this->_set_error('Invalid/Unsupported WHERE clause');
		}
		if(preg_match('/\s+ORDER\s+BY\s+(.*?)(\s+(?:LIMIT).*)?\Z/is', $the_rest, $additional)) {
			$the_rest = isset($additional[2]) ? $additional[2] : '';
			$ORDERBY = explode(',', $additional[1]);
			foreach($ORDERBY as $order_item) {
				if(preg_match('/(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(ASC|DESC))?/is', $order_item, $additional)) {
					list( , $table_alias, $column) = $additional;
					if(!empty($table_alias)) {
						if(!isset($tables[$table_alias])) {
							return $this->_set_error("Unknown table name/alias in ORDER BY: $table_alias");
						}

						$index = array_search($column,  array_keys($joined_info['tables'][$table_alias])) + $joined_info['offsets'][$table_alias];
						if($index === false || $index === null) {
							return $this->_set_error("Unknown column in ORDER BY: $column");
						}
					} else {
						$index = $this->_find_exactly_one($joined_info, $column, "ORDER BY clause");
						if($index === NULL) {
							return NULL;
						}
					}

					$ascend = !empty($additional[3]) ? !strcasecmp('ASC', $additional[3]) : true;
					$this->tosort[] = array('key' => $index, 'ascend' => $ascend);
				}
			}
		}

		if(preg_match('/\s+LIMIT\s+(?:(?:(\d+)\s*,\s*(\-1|\d+))|(?:(\d+)\s+OFFSET\s+(\d+))|(\d+))/is', $the_rest, $additional)) {
			// LIMIT length
			if(isset($additional[5])) {
				$limit_stop = $additional[5]; $limit_start = 0;
			}
			// LIMIT length OFFSET offset (mySQL, Postgres, SQLite)
			else if(isset($additional[3]))
				list(, $limit_stop, $limit_start) = $additional;
			// LIMIT offset, length (mySQL, SQLite)
			else
				list(, $limit_start, $limit_stop) = $additional;

			$limit = array((int) $limit_start, (int) $limit_stop);
		}

		$selected_columns = array();
		$select_line = '';
		foreach($Columns as $column) {
			// function call
			if(preg_match('/\A(`?([^\W\d]\w*)`?\s*\((?:.*?)?\))(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\Z/is', $column, $colmatches)) {
				$function_call = str_replace($colmatches[1], "`", "");
				$function_name = strtolower($colmatches[2]);
				$alias = !empty($colmatches[3]) ? $colmatches[3] : $function_call;
				$expr = $this->_build_expression($select_value, $joined_info, false);
				if($expr !== null)
				{
					$select_line .= $expr.', ';
					$selected_columns[] = $alias;
				}
				else
					return false; // error should already be set by parser
			}
			// identifier/keyword/column/*
			else if(preg_match('/\A(?:`?([^\W\d]\w*)`?\.)?(`?(?:[^\W\d]\w*)|\*)`?(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\Z/is',$column, $colmatches)) {
				list(, $table_name, $column) = $colmatches;
				if($column === '*') {
					if(isset($colmatches[3]))
						return $this->_set_error('Unexpected alias after "*"');

					$star_tables = !empty($table_name) ? array($table_name) : array_keys($tables);
					foreach($star_tables as $tname) {
						$start_index = $joined_info['offsets'][$tname];
						$table_columns = $joined_info['tables'][$tname];
						$column_names = array_keys($table_columns);
						foreach($column_names as $index => $column_name) {
							$select_value = $start_index + $index;
							$select_line .= "\$entry[$select_value], ";
							$selected_columns[] = $column_name;
						}
					}
				} else {
					$alias = !empty($colmatches[3]) ? $colmatches[3] : $column;

					if($table_name) {
						$table_columns = $joined_info['tables'][$table_name];
						$column_names = array_keys($table_columns);
						$index = array_search($column, $column_names) + $joined_info['offsets'][$table_name];
					} else if(strcasecmp($column, 'null')){
						$index = $this->_find_exactly_one($joined_info, $column, "SELECT columns");
						if($index === NULL) {
							return NULL;
						}
						$index = $keys[0];
					} else {  // "null" keyword
						$select_line .= 'NULL, ';
						$selected_columns[] = $column;
						continue;
					}

					$select_line .= "\$entry[$index], ";
					$selected_columns[] = $alias;
				}
			}
			// numeric constant
			else if(preg_match('/\A(-?\d+(?:\.\d+)?)(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\Z/is', $column, $colmatches)) {
				$value = $colmatches[1];
				$alias = !empty($colmatches[2]) ? $colmatches[2] : $value;
				$select_line .= "$value, ";
				$selected_columns[] = $alias;
			}
			// string constant
			else if(preg_match("/\A('(.*?(?<!\\\\))')(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\Z/is", $column, $colmatches)) {
				$value = $colmatches[1];
				$alias = !empty($colmatches[3]) ? $colmatches[3] : $value;
				$select_line .= "$value, ";
				$selected_columns[] = $alias;
			}
			else {
				return $this->_set_error("Parse Error: Unknown value in SELECT clause: $column");
			}
		}

		$line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
		if(!empty($joins))
		{
			if($where !== null)
				$line = "if({$where}) {\r\n\t\t\t\t\t$line\r\n\t\t\t\t}";

			$code = <<<EOT
			foreach(\$data as \$entry) {
				$line
			}
EOT;

		}
		else // Tableless SELECT
		{
			$entry = array(true);  // hack so it passes count and !empty expressions
			$code = $line;
		}

		$final_set = array();
		eval($code);

		if(!empty($this->tosort)) {
			usort($final_set, array($this, "_orderBy"));
		}

		if($limit !== null) {
			$final_set = array_slice($final_set, $limit[0], $limit[1]);
		}

		if(!empty($final_set) && $has_random && preg_match("/\s+RANDOM(?:\((\d+)\)?)?\s+/is", $select, $additional)) {
			$results = array();
			if(!$additional[1]) { $additional[1] = 1; }
			if($additional[1] <= count($this_random)) {
				$random = array_rand($final_set, $additional[1]);
				if(is_array($random)) {	foreach($random as $key) { $results[] = $final_set[$key]; }	}
				else { $results[] = $final_set[$random]; }
			}
			unset($final_set);
			$final_set = $results;
		}

		$rs_id = !empty($this->data) ? max(array_keys($this->data)) + 1 : 1;
		$this->Columns[$rs_id] = $selected_columns;
		$this->cursors[$rs_id] = array(0, 0);
		$this->data[$rs_id] = $final_set;
		return $rs_id;
	}

	function _build_expression($exprStr, $join_info, $where_type = FSQL_WHERE_NORMAL)
	{
		$expr = null;

		// function call
		if(preg_match('/\A([^\W\d]\w*)\s*\((.*?)\)/is', $exprStr, $matches)) {
			$function = strtolower($matches[1]);
			$params = $matches[2];
			$final_param_list = '';
			$paramExprs = array();
			$isCustom = false;
			if(isset($this->renamed_func[$function])) {
				$function = $this->renamed_func[$function];
			}

			if(in_array($function, $this->custom_func)) {
				$isCustom = true;
				$function = "_fsql_functions_".$function;
			} else if(!in_array($function, $this->allow_func)) {
				$this->_set_error("Call to unknown SQL function");
				continue;
			}

			if(strlen($params) !== 0) {
				$parameter = explode(',', $params);
				foreach($parameter as $param) {
					$param = trim($param);

					$paramExpr = $this->_build_expression($param, $join_info, $where_type | 1);
					if($paramExpr === null) // parse error
						return null;

					$paramExprs[] = $paramExpr;
				}
			}

			$final_param_list = implode(',', $paramExprs);

			if($isCustom)
				$expr = "\$this->$function($final_param_list)";
			else
				$expr = "$function($final_param_list)";
		}
		// column/alias/keyword
		else if(preg_match('/\A(?:`?([^\W\d]\w*|\{\{left\}\})`?\.)?`?([^\W\d]\w*)`?\Z/is', $exprStr, $matches)) {
			list( , $table_name, $column) =  $matches;
			// table.column
			if($table_name) {
				if(isset($join_info['tables'][$table_name])) {
					$table_columns = $join_info['tables'][$table_name];
					if(isset($table_columns[ $column ])) {
						$columnData = $table_columns[ $column ];
						if( isset($join_info['offsets'][$table_name]) ) {
							$colIndex = array_search($column,  array_keys($table_columns)) + $join_info['offsets'][$table_name];
							$expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
						} else {
							$colIndex = array_search($column, array_keys($table_columns));
							$expr = "\$right_entry[$colIndex]";
						}
					}
				}
				else if($where_type & FSQL_WHERE_ON && $table_name === '{{left}}')
				{
					$colIndex = $this->_find_exactly_one($joined_info, $column, "expression");
					if($colIndex === NULL) {
						return NULL;
					}
					$expr = "\$left_entry[$colIndex]";
				}
			}
			// null
			else if(!strcasecmp($exprStr, 'NULL')) {
				$expr = 'NULL';
			}
			// unknown
			else if(!strcasecmp($exprStr, 'UNKNOWN')) {
				$expr = 'NULL';
			}
			// true/false
			else if(!strcasecmp($exprStr, 'TRUE') || !strcasecmp($exprStr, 'FALSE')) {
				$expr = strtoupper($exprStr);
			}
			else {  // column/alias
				$colIndex = $this->_find_exactly_one($join_info, $column, "expression");
				if($colIndex === NULL) {
					return NULL;
				}
				$expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
			}
		}
		// number
		else if(preg_match('/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is', $exprStr)) {
			$expr = $exprStr;
		}
		// string
		else if(preg_match("/\A'.*?(?<!\\\\)'\Z/is", $exprStr)) {
			$expr = $exprStr;
		}
		else if(($where_type & FSQL_WHERE_ON) && preg_match('/\A{{left}}\.`?([^\W\d]\w*)`?/is', $exprStr, $matches)) {
			$colIndex = $this->_find_exactly_one($join_info, $column, "expression");
			if($colIndex === NULL) {
				return NULL;
			}

			$expr = "\$left_entry[$colIndex]";
		}
		else
			return null;

		return $expr;
	}

	function _find_exactly_one($join_info, $column, $location) {
		$keys = array_keys($join_info['columns'], $column);
		$keyCount = count($keys);
		if($keyCount == 0) {
			return $this->_set_error("Unknown column/alias in $location: $column");
		} else if($keyCount > 1) {
			return $this->_set_error("Ambiguous column/alias in $location: $column");
		}
		return $keys[0];
	}

	function _build_where($statement, $join_info, $where_type = FSQL_WHERE_NORMAL)
	{
		if($statement) {
			preg_match_all("/(\A\s*|\s+(?:AND|OR)\s+)(NOT\s+)?(\S+?)(\s*(?:!=|<>|>=|<=>?|>|<|=)\s*|\s+(?:IS(?:\s+NOT)?|(?:NOT\s+)?IN|(?:NOT\s+)?R?LIKE|(?:NOT\s+)?REGEXP)\s+)(\((.*?)\)|'.*?'|\S+)/is", $statement, $WHERE, PREG_SET_ORDER);

			if(empty($WHERE))
				return null;

			$condition = '';
			foreach($WHERE as $where)
			{
				$local_condition = '';
				$logicalOp = trim($where[1]);
				$not = !empty($where[2]);
				$leftStr = $where[3];
				$operator = preg_replace('/\s+/', ' ', trim(strtoupper($where[4])));
				$rightStr = $where[5];

				$leftExpr = $this->_build_expression($leftStr, $join_info, $where_type);
				if($leftExpr === null)
					return null;

				if($operator !== 'IN' && $operator !== 'NOT IN')
				{
					$rightExpr = $this->_build_expression($rightStr, $join_info, $where_type);
					if($rightExpr === null)
						return null;

					switch($operator) {
						case '=':
							$local_condition = "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '!=':
						case '<>':
							$local_condition = "(($leftExpr != $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '>':
							if($nullcheck)
							$local_condition = "(($leftExpr > $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '>=':
							if($nullcheck)
							$local_condition = "(($leftExpr >= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<':
							$local_condition = "(($leftExpr < $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<=':
							$local_condition = "(($leftExpr <= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<=>':
							$local_condition = "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case 'IS NOT':
							$not = !$not;
						case 'IS':
							if($rightExpr === 'NULL')
								$local_condition = "($leftExpr === null ? FSQL_TRUE : FSQL_FALSE)";
							else if($rightExpr === 'TRUE')
								$local_condition = "\$this->_fsql_isTrue($leftExpr) ? FSQL_TRUE : FSQL_FALSE)";
							else if($rightExpr === 'FALSE')
								$local_condition = "\$this->_fsql_isFalse($leftExpr) ? FSQL_TRUE : FSQL_FALSE)";
							else
								return null;
							break;
						case 'NOT LIKE':
							$not = !$not;
						case 'LIKE':
							$local_condition = "\$this->_fsql_like($leftExpr, $rightExpr)";
							break;
						case 'NOT RLIKE':
						case 'NOT REGEXP':
							$not = !$not;
						case 'RLIKE':
						case 'REGEXP':
							$local_condition = "\$this->_fsql_regexp($leftExpr, $rightExpr)";
							break;
						default:
							$local_condition = "$leftExpr $operator $rightExpr";
							break;
					}
				}
				else
				{
					if(!empty($where[6])) {
						$array_values = explode(',', $where[6]);
						$valuesExpressions = array();
						foreach($array_values as $value)
						{
							$valueExpr = $this->_build_expression(trim($value), $join_info, $where_type);
							$valuesExpressions[] = $valueExpr['expression'];
						}
						$valuesString = implode(',', $valuesExpressions);
						$local_condition = "\$this->_fsql_in($leftExpr, array($valuesString))";

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
					$condition .= '\$this->_fsql_not('.$local_condition.')';
				else
					$condition .= $local_condition;
			}
			return "($condition) === ".FSQL_TRUE;
		}
		return null;
	}

	function _orderBy($a, $b)
	{
		foreach($this->tosort as $tosort) {
			extract($tosort);
			$a[$key] = preg_replace("/^'(.+?)'$/", "\\1", $a[$key]);
			$b[$key] = preg_replace("/^'(.+?)'$/", "\\1", $b[$key]);
			if (($a[$key] > $b[$key] && $ascend) || ($a[$key] < $b[$key] && !$ascend)) {
				return 1;
			} else if (($a[$key] < $b[$key] && $ascend) || ($a[$key] > $b[$key] && !$ascend)) {
				return -1;
			}
		}
	}

	////Delete data from the DB
	function _query_delete($query)
	{
		$this->affected  = 0;
		if(preg_match("/\ADELETE\s+FROM\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)(?:\s+(WHERE\s+.+))?\s*[;]?\Z/is", $query, $matches)) {
			list(, $db_name, $table_name) = $matches;

			if(!$db_name)
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$db_name];

			$table =& $db->getTable($table_name);
			if(!$table->exists()) {
				$this->_error_table_not_exists($db->name, $table_name);
				return NULL;
			}
			elseif($table->isReadLocked()) {
				$this->_error_table_read_lock($db->name, $table_name);
				return null;
			}

			$columns = $table->getColumns();
			$cursor =& $table->getCursor();

			if($cursor->isDone())
				return true;

			if(isset($matches[3]) && preg_match("/^WHERE ((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/is", $matches[3], $first_where))
			{
				$where = $this->_load_where($first_where[1], false);
				if(!$where) {
					$this->_set_error("Invalid/Unsupported WHERE clause");
					return null;
				}

				$alter_columns = array();
				foreach($columns as $column => $columnDef) {
					if($columnDef['type'] == 'e')
						$alter_columns[] = $column;
				}

				for($k = 0; !$cursor->isDone(); $k++, $cursor->next()) {

					$entry = $cursor->getRow();
					foreach($alter_columns as $column) {
						if($columns[$column]['type'] == 'e') {
							$i = $entry[$column];
							$entry[$column] = ($i == 0) ? "''" : $columns[$column]['restraint'][$i - 1];
						}
					}

					$proceed = "";
					for($i = 0; $i < count($where); $i++) {
						if($i > 0 && $where[$i - 1]["next"] == "AND") {
							$proceed .= " && ".$this->_where_functions($where[$i], $entry, $table_name);
						}
						else if($i > 0 && $where[$i - 1]["next"] == "OR") {
							$proceed .= " || ".$this->_where_functions($where[$i], $entry, $table_name);
						}
						else {
							$proceed .=  intval($this->_where_functions($where[$i], $entry, $table_name) == 1);
						}
					}
					eval("\$cond = $proceed;");
					if(!($cond))
						continue;

					$table->deleteRow($k);

					$this->affected++;
				}
			} else {
				for($k = 0; !$cursor->isDone(); $k++, $cursor->next())
					$table->deleteRow($k);
				$this->affected = $k;
			}

			if($this->affected)
			{
				if($this->auto)
					$table->commit();
				else if(!in_array($table, $this->updatedTables))
					$this->updatedTables[] =& $table;
			}

			return TRUE;
		} else {
			$this->_set_error("Invalid DELETE query");
			return null;
		}
	}

	function _query_alter($query)
	{
		if(preg_match("/\AALTER\s+TABLE\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s+(.*)/is", $query, $matches)) {
			list(, $db_name, $table_name, $changes) = $matches;

			if(!$db_name)
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$db_name];

			$tableObj =& $db->getTable($table_name);
			if(!$tableObj->exists()) {
				$this->_error_table_not_exists($db->name, $table_name);
				return NULL;
			}
			elseif($tableObj->isReadLocked()) {
				$this->_error_table_read_lock($db->name, $table_name);
				return null;
			}
			$columns =  $tableObj->getColumns();

			$table = $this->_load_table($db, $table_name);

			preg_match_all("/(?:ADD|ALTER|CHANGE|DROP|RENAME).*?(?:,|\Z)/is", trim($changes), $specs);
			for($i = 0; $i < count($specs[0]); $i++) {
				if(preg_match("/\AADD\s+(?:CONSTRAINT\s+`?[A-Z][A-Z0-9\_]*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/is", $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];

					foreach($columns as $name => $column) {
						if($column['key'] == 'p') {
							$this->_set_error("Primary key already exists");
							return NULL;
						}
					}

					$columnDef['key'] = 'p';
					$tableObj->setColumns($columns);

					return true;
				} else if(preg_match("/\ACHANGE(?:\s+(?:COLUMN))?\s+`?([A-Z][A-Z0-9\_]*)`?\s+(?:SET\s+DEFAULT ((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\3))|DROP\s+DEFAULT)(?:,|;|\Z)/is", $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];
					if(isset($matches[2]))
						$default = $matches[2];
					else
						$default = "NULL";

					if(!$columnDef['null'] && strcasecmp($default, "NULL")) {
						if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
							if($columnDef['type'] == 'i')
								$default = intval($matches[2]);
							else if($columnDef['type'] == 'f')
								$default = floatval($matches[2]);
							else if($columnDef['type'] == 'e') {
								if(in_array($default, $columnDef['restraint']))
									$default = array_search($default, $columnDef['restraint']) + 1;
								else
									$default = 0;
							}
						} else {
							if($columnDef['type'] == 'i')
								$default = intval($default);
							else if($columnDef['type'] == 'f')
								$default = floatval($default);
							else if($columnDef['type'] == 'e') {
								$default = intval($default);
								if($default < 0 || $default > count($columnDef['restraint'])) {
									$this->_set_error("Numeric ENUM value out of bounds");
									return NULL;
								}
							}
						}
					} else if(!$columnDef['null']) {
						if($columnDef['type'] == 's')
							// The default for string types is the empty string
							$default = "''";
						else
							// The default for dates, times, and number types is 0
							$default = 0;
					}

					$columnDef['default'] = $default;
					$tableObj->setColumns($columns);

					return true;
				} else if(preg_match("/\ADROP\s+PRIMARY\s+KEY/is", $specs[0][$i], $matches)) {
					$found = false;
					foreach($columns as $name => $column) {
						if($column['key'] == 'p') {
							$columns[$name]['key'] = 'n';
							$found = true;
						}
					}

					if($found) {
						$tableObj->setColumns($columns);
						return true;
					} else {
						$this->_set_error("No primary key found");
						return NULL;
					}
				}
				else if(preg_match("/\ARENAME\s+(?:TO\s+)?`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $specs[0][$i], $matches)) {
					list(, $new_db_name, $new_table_name) = $matches;

					if(!$new_db_name)
						$new_db =& $this->currentDB;
					else
						$new_db =& $this->databases[$new_db_name];

					$new_table =& $new_db->getTable($new_table_name);
					if($new_table->exists()) {
						$this->_set_error("Destination table {$new_db_name}.{$new_table_name} already exists");
						return NULL;
					}

					return $db->renameTable($old_table_name, $new_table_name, $new_db);
				}
				else {
					$this->_set_error("Invalid ALTER query");
					return null;
				}
			}
		} else {
			$this->_set_error("Invalid ALTER query");
			return null;
		}
	}

	function _query_rename($query)
	{
		if(preg_match("/\ARENAME\s+TABLE\s+(.*)\s*[;]?\Z/is", $query, $matches)) {
			$tables = explode(",", $matches[1]);
			foreach($tables as $table) {
				list($old, $new) = preg_split("/\s+TO\s+/i", trim($table));

				if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $old, $table_parts)) {
					list(, $old_db_name, $old_table_name) = $table_parts;

					if(!$old_db_name)
						$old_db =& $this->currentDB;
					else
						$old_db =& $this->databases[$old_db_name];
				} else {
					$this->_set_error("Parse error in table listing");
					return NULL;
				}

				if(preg_match("/(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)/is", $new, $table_parts)) {
					list(, $new_db_name, $new_table_name) = $table_parts;

					if(!$new_db_name)
						$new_db =& $this->currentDB;
					else
						$new_db =& $this->databases[$new_db_name];
				} else {
					$this->_set_error("Parse error in table listing");
					return NULL;
				}

				$old_table =& $old_db->getTable($old_table_name);
				if(!$old_table->exists()) {
					$this->_error_table_not_exists($old_db_name, $old_table_name);
					return NULL;
				}
				elseif($old_table->isReadLocked()) {
					$this->_error_table_read_lock($old_db_name, $old_table_name);
					return null;
				}

				$new_table =& $new_db->getTable($new_table_name);
				if($new_table->exists()) {
					$this->_set_error("Destination table {$new_db_name}.{$new_table_name} already exists");
					return NULL;
				}

				return $old_db->renameTable($old_table_name, $new_table_name, $new_db);
			}
			return TRUE;
		} else {
			$this->_set_error("Invalid RENAME query");
			return null;
		}
	}

	function _query_drop($query)
	{
		if(preg_match("/\ADROP(?:\s+(TEMPORARY))?\s+TABLE(?:\s+(IF EXISTS))?\s+(.*)\s*[;]?\Z/is", $query, $matches)) {
			$temporary = !empty($matches[1]);
			$ifexists = !empty($matches[2]);
			$tables = explode(",", $matches[3]);

			foreach($tables as $table) {
				if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_parts)) {
					list(, $db_name, $table_name) = $table_parts;

					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];

					$table =& $db->getTable($table_name);
					if($table->isReadLocked()) {
						$this->_error_table_read_lock($db->name, $table_name);
						return null;
					}

					$existed = $db->dropTable($table_name);
					if(!$ifexists && !$existed) {
						$this->_error_table_not_exists($db->name, $table_name);
						return null;
					}
				} else {
					$this->_set_error("Parse error in table listing");
					return NULL;
				}
			}
			return TRUE;
		} else if(preg_match("/\ADROP\s+DATABASE(?:\s+(IF EXISTS))?\s+`?([A-Z][A-Z0-9\_]*)`?s*[;]?\Z/is", $query, $matches)) {
			$ifexists = !empty($matches[1]);
			$db_name = $matches[2];

			if(!$ifexists && !isset($this->databases[$db_name])) {
				$this->_set_error("Database '{$db_name}' does not exist");
				return null;
			} else if(!isset($this->databases[$db_name])) {
				return true;
			}

			$db =& $this->databases[$db_name];

			$tables = $db->listTables();

			foreach($tables as $table) {
				$db->dropTable($table_name);
			}

			unset($this->databases[$db_name]);

			return TRUE;
		} else {
			$this->_set_error("Invalid DROP query");
			return null;
		}
	}

	function _query_truncate($query)
	{
		if(preg_match("/\ATRUNCATE\s+TABLE\s+(.*)[;]?\Z/is", $query, $matches)) {
			$tables = explode(",", $matches[1]);
			foreach($tables as $table) {
				if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $matches)) {
					list(, $db_name, $table_name) = $matches;

					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];

					$table =& $db->getTable($table_name);
					if($table->exists()) {
						if($table->isReadLocked()) {
							$this->_error_table_read_lock($db->name, $table_name);
							return null;
						}
						$columns = $table->getColumns();
						$db->dropTable($table_name);
						$db->createTable($table_name, $columns);
					} else {
						return NULL;
					}
				} else {
					$this->_set_error("Parse error in table listing");
					return NULL;
				}
			}
		} else {
			$this->_set_error("Invalid TRUNCATE query");
			return NULL;
		}

		return true;
	}

	function _query_backup($query)
	{
		if(!preg_match("/\ABACKUP TABLE (.*?) TO '(.*?)'\s*[;]?\Z/is", $query, $matches)) {
			if(substr($matches[2], -1) != "/")
				$matches[2] .= '/';

			$tables = explode(",", $matches[1]);
			foreach($tables as $table) {
				if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_name_matches)) {
					list(, $db_name, $table_name) = $table_name_matches;

					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];

					$db->copyTable($table_name, $db->path_to_db, $matches[2]);
				} else {
					$this->_set_error("Parse error in table listing");
					return NULL;
				}
			}
		} else {
			$this->_set_error("Invalid Query");
			return NULL;
		}
	}

	function _query_restore($query)
	{
		if(!preg_match("/\ARESTORE TABLE (.*?) FROM '(.*?)'\s*[;]?\s*\Z/is", $query, $matches)) {
			if(substr($matches[2], -1) != "/")
				$matches[2] .= '/';

			$tables = explode(",", $matches[1]);
			foreach($tables as $table) {
				if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_name_matches)) {
					list(, $db_name, $table_name) = $table_name_matches;

					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];

					$db->copyTable($table_name, $matches[2], $db->path_to_db);
				} else {
					$this->_set_error("Parse error in table listing");
					return NULL;
				}
			}
		} else {
			$this->_set_error("Invalid Query");
			return NULL;
		}
	}

	function _query_show($query)
	{
		if(preg_match("/\ASHOW\s+(FULL\s+)?TABLES(?:\s+FROM\s+`?([A-Z][A-Z0-9\_]*)`?)?\s*[;]?\s*\Z/is", $query, $matches)) {
			$randval = rand();
			$full = !empty($matches[1]);

			if(!$matches[2])
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$matches[2]];

			$tables = $db->listTables();
			$data = array();

			foreach($tables as $table_name) {
				$table_name = '\''.$table_name.'\'';
				if($full) {
					$data[] = array("Name" => $table_name, 'Table_type' => 'BASE TABLE');
				} else {
					$data[] = array("Name" => $table_name);
				}
			}

			$columns = array("Name");
			if($full) {
				$columns[] = 'Table_type';
			}

			$this->Columns[$randval] = $columns;
			$this->cursors[$randval] = array(0, 0);
			$this->data[$randval] = $data;

			return $randval;
		} else if(preg_match("/\ASHOW\s+DATABASES\s*[;]?\s*\Z/is", $query, $matches)) {
			$randval = rand();

			$dbs = array_keys($this->databases);
			foreach($dbs as $db) {
				$db = '\''.$db.'\'';
				$data[] = array("name" => $db);
			}

			$this->Columns[$randval] = array("name");
			$this->cursors[$randval] = array(0, 0);
			$this->data[$randval] = $data;

			return $randval;
		} else if(preg_match('/\ASHOW\s+(FULL\s+)?COLUMNS\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?(?:\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?)?\s*[;]?\s*\Z/is', $query, $matches)) {
			return $this->_show_columns($matches[3], $matches[2], !empty($matches[1]));
		 } else {
			$this->_set_error("Invalid SHOW query");
			return NULL;
		}
	}

	function _show_columns($db_name, $table_name, $full)
	{
		$randval = rand();

		if(!$db_name)
			$db =& $this->currentDB;
		else
			$db =& $this->databases[$db_name];

		$tableObj =& $db->getTable($table_name);
		if(!$tableObj->exists()) {
			$this->_error_table_not_exists($db->name, $matches[2]);
			return NULL;
		}
		$columns =  $tableObj->getColumns();

		$data = array();

		foreach($columns as $name => $column) {
			$name = '\''.$name.'\'';
			$type = $this->_typecode_to_name($column['type']);
			$null = ($column['null']) ? "'YES'" : "''";
			$extra = ($column['auto']) ? "'auto_increment'" : "''";

			if($column['key'] == 'p')
				$key = "'PRI'";
			else if($column['key'] == 'u')
				$key = "'UNI'";
			else
				$key = "''";

			if(!$full) {
				$row = array("Field" => $name, "Type" => "'$type'", "Null" => $null, "Default" => $column['default'], "Key" => $key, "Extra" => $extra);
			} else {
				$row = array("Field" => $name, "Type" => "'$type'", 'Collation' => "NULL", "Null" => $null, "Default" => $column['default'], "Key" => $key,
					"Extra" => $extra, "Privileges" => "'select,insert,update,references'", "Comment" => "''");
			}

			$data[] = $row;
		}

		$this->Columns[$randval] = array_keys($data[0]);
		$this->cursors[$randval] = array(0, 0);
		$this->data[$randval] = $data;

		return $randval;
	}

	function _query_describe($query)
	{
		if(preg_match("/\ADESC(?:RIBE)?\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s*[;]?\s*\Z/is", $query, $matches)) {
			return $this->_show_columns($matches[1], $matches[2], false);
		} else {
			$this->_set_error('Invalid DESCRIBE query');
			return NULL;
		}
	}

	function _query_use($query)
	{
		if(preg_match("/\AUSE\s+`?([A-Z][A-Z0-9\_]*)`?\s*[;]?\s*\Z/is", $query, $matches)) {
			$this->select_db($matches[1]);
			return TRUE;
		} else {
			$this->_set_error('Invalid USE query');
			return NULL;
		}
	}

	function _query_lock($query)
	{
		if(preg_match("/\ALOCK\s+TABLES\s+(.+?)\s*[;]?\s*\Z/is", $query, $matches)) {
			preg_match_all("/(?:([A-Z][A-Z0-9\_]*)`?\.`?)?`?([A-Z][A-Z0-9\_]*)`?\s+((?:READ(?:\s+LOCAL)?)|((?:LOW\s+PRIORITY\s+)?WRITE))/is", $matches[1], $rules);
			$numRules = count($rules[0]);
			for($r = 0; $r < $numRules; $r++) {
				if(!$rules[1][$r])
					$db =& $this->currentDB;
				else
					$db =& $this->databases[$rules[1][$r]];

				$table_name = $rules[2][$r];
				$table =& $db->getTable($table_name);
				if(!$table->exists()) {
					$this->_error_table_not_exists($db->name, $table_name);
					return NULL;
				}

				if(!strcasecmp(substr($rules[3][$r], 0, 4), "READ")) {
					$table->readLock();
				}
				else {  /* WRITE */
					$table->writeLock();
				}

				$lockedTables[] =& $table;
			}
			return TRUE;
		} else {
			$this->_set_error('Invalid LOCK query');
			return NULL;
		}
	}

	function _query_unlock($query)
	{
		if(preg_match("/\AUNLOCK\s+TABLES\s*[;]?\s*\Z/is", $query)) {
			$this->_unlock_tables();
			return TRUE;
		} else {
			$this->_set_error('Invalid UNLOCK query');
			return NULL;
		}
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
					$this->_set_error('Invalid integer value for insert');
					return false;
				}
			case FSQL_TYPE_FLOAT:
				if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
					return (float) $matches[1];
				}
				else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
					return (float) $value;
				}
				else {
					$this->_set_error('Invalid float value for insert');
					return false;
				}
			case FSQL_TYPE_ENUM:
				if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $matches)) {
					$value = $matches[1];
				}

				if(in_array($value, $columnDef['restraint']) || strlen($value) === 0) {
					return $value;
				} else if(is_numeric($value)) {
					$index = (int) $value;
					if($index >= 1 && $index <= count($columnDef['restraint'])) {
						return $columnDef['restraint'][$index - 1];
					} else if($index === 0) {
						return "";
					} else {
						$this->_set_error('Numeric ENUM value out of bounds');
						return false;
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

		return false;
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
		$right_padding = array_fill(0, $pad_length, null);

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
		$left_padding = array_fill(0, $pad_length, null);

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
		$left_padding = array_fill(0, $left_pad_length, null);
		$right_padding = array_fill(0, $right_pad_length, null);

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

	function fetch_array($id, $type = 1)
	{
		if(!$id || !isset($this->cursors[$id]) || !isset($this->data[$id][$this->cursors[$id][0]]))
			return NULL;

		$entry = $this->data[$id][$this->cursors[$id][0]];
		if(!$entry)
			return NULL;

		$columnNames = $this->Columns[$id];

		$this->cursors[$id][0]++;

		if($type === FSQL_ASSOC) {  return array_combine($columnNames, $entry); }
		else if($type === FSQL_NUM) { return $entry; }
		else{ return array_merge($entry, array_combine($columnNames, $entry)); }
	}

	function fetch_assoc($results) { return $this->fetch_array($results, FSQL_ASSOC); }
	function fetch_row	($results) { return $this->fetch_array($results, FSQL_NUM); }
	function fetch_both	($results) { return $this->fetch_array($results, FSQL_BOTH); }

	function fetch_single($results, $column = 0) {
		$type = is_numeric($column) ? FSQL_NUM : FSQL_ASSOC;
		$row = $this->fetch_array($results, $type);
		return $row != NULL ? $row[$column] : false;
	}

	function fetch_object($results)
	{
		$row = $this->fetch_array($results, FSQL_ASSOC);

		if($row == NULL)
			return NULL;

		$obj =& new stdClass();

		foreach($row as $key => $value)
			$obj->{$key} = $value;

		return $obj;
	}

	function data_seek($id, $i)
	{
		if(!$id || !isset($this->cursors[$id][0])) {
			$this->_set_error("Bad results id passed in");
			return false;
		} else {
			$this->cursors[$id][0] = $i;
			return true;
		}
	}

	function num_fields($id)
	{
		if(!$id || !isset($this->Columns[$id])) {
			$this->_set_error("Bad results id passed in");
			return false;
		} else {
			return count($this->Columns[$id]);
		}
	}

	function fetch_field($id, $i = NULL)
	{
		if(!$id || !isset($this->Columns[$id]) || !isset($this->cursors[$id][1])) {
			$this->_set_error("Bad results id passed in");
			return false;
		} else {
			if($i == NULL)
				$i = 0;

			if(!isset($this->Columns[$id][$i]))
				return null;

			$field = new stdClass();
			$field->name = $this->Columns[$id][$i];
			return $field;
		}
	}

	function free_result($id)
	{
		unset($this->Columns[$id], $this->data[$id], $this->cursors[$id]);
	}

	function _typecode_to_name($type)
	{
		switch($type)
		{
			case FSQL_TYPE_DATE:				return 'DATE';
			case FSQL_TYPE_DATETIME:			return 'DATETIME';
			case FSQL_TYPE_ENUM:				return 'ENUM';
			case FSQL_TYPE_FLOAT:				return 'DOUBLE';
			case FSQL_TYPE_INTEGER:				return 'INTEGER';
			case FSQL_TYPE_STRING:				return 'TEXT';
			case FSQL_TYPE_TIME:				return 'TIME';
			default:							return false;
		}
	}

	function _fsql_strip_stringtags($string)
	{
		return preg_replace("/^'(.+)'$/s", "\\1", $string);
	}

	// operators

	function _fsql_not($x)
	{
		$c = ~$x & 3;
		return (($c << 1) ^ ($c >> 1)) & 3;
	}

	function _fsql_isTrue($expr)
	{
		return !in_array($expr, array(0, 0.0, '', null), true);
	}

	function _fsql_isFalse($expr)
	{
		return in_array($expr, array(0, 0.0, ''), true);
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
			return (preg_match('/'.$right.'/i', $left)) ? FSQL_TRUE : FSQL_FALSE;
		else
			return FSQL_UNKNOWN;
	}

	//////Misc Functions
	function _fsql_functions_database()
	{
		return $this->currentDB->name;
	}

	function _fsql_functions_last_insert_id()
	{
		return $this->insert_id;
	}

	function _fsql_functions_row_count()
	{
		return $this->affected;
	}

	/////Math Functions
	function _fsql_functions_log($arg1, $arg2 = NULL) {
		$arg1 = $this->_fsql_strip_stringtags($arg1);
		if($arg2) {
			$arg2 = $this->_fsql_strip_stringtags($arg2);
		}
		if(($arg1 < 0 || $arg1 == 1) && !$arg2) { return NULL; }
		if(!$arg2) { return log($arg1); } else { return log($arg2) / log($arg1); }
	}
	function _fsql_functions_log2($arg)
	{
		$arg = $this->_fsql_strip_stringtags($arg);
		return $this->_fsql_functions_log(2, $arg);
	}
	function _fsql_functions_log10($arg) {
		$arg = $this->_fsql_strip_stringtags($arg);
		return $this->_fsql_functions_log(10, $arg);
	}
	function _fsql_functions_mod($one, $two) {
		$one = $this->_fsql_strip_stringtags($one);
		$two = $this->_fsql_strip_stringtags($two);
		return $one % $two;
	}
	function _fsql_functions_sign($number) {
		$number = $this->_fsql_strip_stringtags($number);
		if($number > 0) { return 1; } else if($number == 0) { return 0; } else { return -1; }
	}
	function _fsql_functions_truncate($number, $places) {
		$number = $this->_fsql_strip_stringtags($number);
		$places = round($this->_fsql_strip_stringtags($number));
		list($integer, $decimals) = explode(".", $number);
		if($places == 0) { return $integer; }
		else if($places > 0) { return $integer.'.'.substr($decimals,0,$places); }
		else {   return substr($number,0,$places) * pow(10, abs($places));  }
	}

	 /////Grouping and other Misc. Functions
	function _fsql_functions_count($column, $id) {
		if($column == "*") { return count($this->data[$id]); }
		else {   $i = 0;   foreach($this->data[$id] as $entry) {  if($entry[$column]) { $i++; } }  return $i;  }
	}
	function _fsql_functions_max($column, $id) {
		foreach($this->data[$id] as $entry){   if($entry[$column] > $i || !$i) { $i = $entry[$column]; }  }	return $i;
	}
	function _fsql_functions_min($column, $id) {
		foreach($this->data[$id] as $entry){   if($entry[$column] < $i || !$i) { $i = $entry[$column]; }  }	return $i;
	}
	function _fsql_functions_sum($column, $id) {  foreach($this->data[$id] as $entry){ $i += $entry[$column]; }  return $i; }

	 /////String Functions
	function _fsql_functions_concat_ws($string) {
		$numargs = func_num_args();
		if($numargs > 2) {
			for($i = 1; $i < $numargs; $i++) { $return[] = func_get_arg($i);  }
			return implode($string, $return);
		}
		else { return NULL; }
	}
	function _fsql_functions_concat() { return call_user_func_array(array($this,'_fsql_functions_concat_ws'), array("",func_get_args())); }
	function _fsql_functions_elt() {
		$return = func_get_arg(0);
		if(func_num_args() > 1 && $return >= 1 && $return <= func_num_args()) {	return func_get_arg($return);  }
		else { return NULL; }
	}
	function _fsql_functions_locate($string, $find, $start = NULL) {
		if($start) { $string = substr($string, $start); }
		$pos = strpos($string, $find);
		if($pos === false) { return 0; } else { return $pos; }
	}
	function _fsql_functions_lpad($string, $length, $pad) { return str_pad($string, $length, $pad, STR_PAD_LEFT); }
	function _fsql_functions_left($string, $end)	{ return substr($string, 0, $end); }
	function _fsql_functions_right($string,$end)	{ return substr($string, -$end); }
	function _fsql_functions_substring_index($string, $delim, $count) {
		$parts = explode($delim, $string);
		if($count < 0) {   for($i = $count; $i > 0; $i++) { $part = count($parts) + $i; $array[] = $parts[$part]; }  }
		else { for($i = 0; $i < $count; $i++) { $array[] = $parts[$i]; }  }
		return implode($delim, $array);
	}

	////Date/Time functions
	function _fsql_functions_now()		{ return $this->_fsql_functions_from_unixtime(time()); }
	function _fsql_functions_curdate()	{ return $this->_fsql_functions_from_unixtime(time(), "%Y-%m-%d"); }
	function _fsql_functions_curtime() 	{ return $this->_fsql_functions_from_unixtime(time(), "%H:%M:%S"); }
	function _fsql_functions_dayofweek($date) 	{ return $this->_fsql_functions_from_unixtime($date, "%w"); }
	function _fsql_functions_weekday($date)		{ return $this->_fsql_functions_from_unixtime($date, "%u"); }
	function _fsql_functions_dayofyear($date)		{ return round($this->_fsql_functions_from_unixtime($date, "%j")); }
	function _fsql_functions_unix_timestamp($date = NULL) {
		if(!$date) { return NULL; } else { return strtotime(str_replace("-","/",$date)); }
	}
	function _fsql_functions_from_unixtime($timestamp, $format = "%Y-%m-%d %H:%M:%S")
	{
		if(!is_int($timestamp)) { $timestamp = $this->_fsql_functions_unix_timestamp($timestamp); }
		return strftime($format, $timestamp);
	}

}

?>
