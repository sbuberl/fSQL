<?php 

define('FSQL_ASSOC',1,TRUE);
define('FSQL_NUM',  2,TRUE);
define('FSQL_BOTH', 3,TRUE);

define('FSQL_TRUE', ~0, TRUE);
define('FSQL_FALSE', 0,TRUE);
define('FSQL_NULL', 1,TRUE);
define('FSQL_UNKNOWN', 1,TRUE);

define('FSQL_JOIN_INNER',0,TRUE);
define('FSQL_JOIN_LEFT',1,TRUE);
define('FSQL_JOIN_RIGHT',2,TRUE);
define('FSQL_JOIN_FULL',3,TRUE);

define('FSQL_EXTENSION', '.cgi',TRUE);

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

/**
 *  A reentrant read/write lock system for opening a file
 * 
 */
class fSQLFile
{
	var $handle;
	var $filepath;
	var $lock;
	var $rcount = 0;
	var $wcount = 0;

	function fSQLFile($filepath)
	{
		$this->filepath = $filepath;
		$this->handle = null;
		$this->lock = 0;
	}

	function getHandle()
	{
		return $this->handle;
	}

	function getPath()
	{
		return $this->filepath;
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
			//	flock($this->handle, LOCK_UN);
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
					//	flock($this->handle, LOCK_UN);
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

class fSQLCursor
{
	var $entries;
	var $current_row_id;

	function fSQLCursor(&$entries)
	{
		$this->entries =& $entries;
		$this->first();
	}

	function first()
	{
		reset($this->entries);
		$this->current_row_id = key($this->entries);
		return $this->current_row_id;
	}
	
	function getRow()
	{
		return $this->current_row_id !== false ? $this->entries[$this->current_row_id] : null;
	}
	
	function isDone()
	{
		return $this->current_row_id !== false;
	}
	
	function last()
	{
		end($this->entries);
		$this->current_row_id = key($this->entries);
		return $this->current_row_id;
	}
	
	function previous()
	{
		prev($this->entries);
		$this->current_row_id = key($this->entries);
		return $this->current_row_id;
	}
	
	function next()
	{
		next($this->entries);
		$this->current_row_id = key($this->entries);
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

class fSQLWriteCursor extends fSQLTableCursor
{
	var $uncommited = false;

	function fSQLWriteCursor(&$entries)
	{
		$this->entries =& $entries;
		$this->first();
	}

	function updateField($column, $value)
	{
		if($this->current_row_id !== false)
		{
			$this->entries[$this->current_row_id][$column] = $value;
			$this->uncommited = true;
		}
	}

	function deleteRow()
	{
		if($this->current_row_id !== false)
		{
			unset($this->entries[$this->current_row_id]);
			$this->uncommited = true;
		}
	}
}

class fSQLTable
{
	var $name = NULL;
	var $database = NULL;

	function fSQLTable($name, &$database)
	{
		$this->name = $name;
		$this->database =& $database;
	}

	function getName()
	{
		return $this->name;
	}

	function getDatabase()
	{
		return $this->database;
	}

	function exists() { return false; }

	function rename($new_name)
	{
		$this->name = $new_name;
	}

	function drop() { return false; }

	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
}

class fSQLTemporaryTable extends fSQLTable
{
	var $rcursor = NULL;
	var $wcursor = NULL;
	var $columns = NULL;
	var $entries = NULL;
	var $data_load =0;
	var $uncommited = false;

	function fSQLTemporaryTable($name, &$database)
	{
		$this->name = $name;
		$this->database =& $database;
	}

	function create($columnDefs)
	{
		$table->columns = $columnDefs;
	}
	
	function exists() {
		return true;
	}

	function temporary() {
		return true;
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
	
	function &getCursor()
	{
		if($this->rcursor == NULL)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}

	function &getWriteCursor()
	{
		if($this->wcursor == NULL)
			$this->wcursor =& new fSQLWriteCursor($this->entries);
		
		return $this->wcursor;
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

	// Free up all data
	function drop()
	{
		$this->rcursor = NULL;
		$this->wcursor = NULL;
		$this->columns = NULL;
		$this->entries = NULL;
		$this->data_load = 0;
		$this->uncommited = false;
	}

	/* Unnecessary for temporary tables */
	function isReadLocked() { return false; }
	function readLock() { return true; }
	function writeLock() { return true; }
	function unlock() { return true; }
}

class fSQLStandardTable extends fSQLTable
{
	var $cursor = NULL;
	var $columns = NULL;
	var $entries = NULL;
	var $columns_load = NULL;
	var $data_load = NULL;
	var $uncommited = false;
	var $columnsLockFile = NULL;
	var $columnsFile = NULL;
	var $dataLockFile = NULL;
	var $dataFile = NULL;
	var $lock = NULL;
	
	function fSQLStandardTable($name, &$database)
	{
		$this->name = $name;
		$this->database =& $database;
		$path_to_db = $database->getPath();
		$columns_path = $path_to_db.$name.'.columns';
		$data_path = $path_to_db.$name.'.data';
		$this->columnsLockFile = new fSQLFile($columns_path.'.lock.cgi');
		$this->columnsFile = new fSQLFile($columns_path.'.cgi');
		$this->dataLockFile = new fSQLFile($data_path.'.lock.cgi');
		$this->dataFile = new fSQLFile($data_path.'.cgi');
	}
	
	function create($columnDefs)
	{
		$this->columns = $columnDefs;
		
		list($msec, $sec) = explode(' ', microtime());
		$this->columns_load = $this->data_load = $sec.$msec;

		// create the columns lock
		$this->columnsLockFile->acquireWrite();
		$columnsLock = $this->columnsLockFile->getHandle();
		ftruncate($columnsLock, 0);
		fwrite($columnsLock, $this->columns_load);
		
		// create the columns file
		$this->columnsFile->acquireWrite();
		$toprint = $this->_printColumns($columnDefs);
		fwrite($this->columnsFile->getHandle(), $toprint);
		
		$this->columnsFile->releaseWrite();	
		$this->columnsLockFile->releaseWrite();

		// create the data lock
		$this->dataLockFile->acquireWrite();
		$dataLock = $this->dataLockFile->getHandle();
		ftruncate($dataLock, 0);
		fwrite($dataLock, $this->data_load);
		
		// create the data file
		$this->dataFile->acquireWrite();
		fwrite($this->dataFile->getHandle(), "0\r\n");
		
		$this->dataFile->releaseWrite();	
		$this->dataLockFile->releaseWrite();
		
		return $this;
	}

	function _printColumns($columnDefs)
	{
		$toprint = count($columnDefs)."\r\n";
		foreach($columnDefs as $name => $column) {
			if(!is_array($column['restraint'])) {
				$toprint .= $name.': '.$column['type'].';;'.$column['auto'].';'.$column['default'].';'.$column['key'].';'.$column['null'].";\r\n";
			} else {
				$toprint .= $name.': '.$column['type'].';'.implode(',', $column['restraint']).';'.$column['auto'].';'.$column['default'].';'.$column['key'].';'.$column['null'].";\r\n";
			}
		};
		return $toprint;
	}
	
	function exists() {
		return file_exists($this->columnsFile->getPath());
	}

	function temporary() {
		return false;
	}
	
	function getColumnNames() {
		return array_keys($this->getColumns());
	}
	
	function getColumns() {
		$this->columnsLockFile->acquireRead();
		$lock = $this->columnsLockFile->getHandle();

		$modified = fread($lock, 20);
		if($this->columns_load === NULL || $this->columns_load < $modified)
		{
			$this->columns_load = $modified;
			
			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();

			$line = fgets($columnsHandle);			
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return NULL;
			}
			
			$num_columns = $matches[1];

			for($i = 0; $i < $num_columns; $i++) {
				$line =	fgets($columnsHandle);
				if(preg_match("/(\S+): (dt|d|i|f|s|t|e);(.*);(0|1);(-?\d+(?:\.\d+)?|'.*'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
					$type = $matches[2];
					$default = $matches[5];
					if($default === "NULL")
						$default = NULL;
					else if($type === 'i')
						$default = (int) $default;
					else if($type === 'f')
						$default = (float) $default;
					$this->columns[$matches[1]] = array(
						'type' => $type, 'auto' => $matches[4], 'default' => $default, 'key' => $matches[6], 'null' => $matches[7]
					);
					preg_match_all("/'.*?(?<!\\\\)'/", $matches[3], $this->columns[$matches[1]]['restraint']);
					$this->columns[$matches[1]]['restraint'] = $this->columns[$matches[1]]['restraint'][0];
				} else {
					return NULL;
				}
			}

			$this->columnsFile->releaseRead();
		}
		
		$this->columnsLockFile->releaseRead();
		
		return $this->columns;
	}
	
	function &getCursor()
	{
		$this->_loadEntries();

		if($this->cursor === NULL)
			$this->cursor = new fSQLCursor($this->entries);
		
		return $this->cursor;
	}
	
	function &getWriteCursor()
	{
		$this->_loadEntries();

		if($this->cursor === NULL)
			$this->cursor = new fSQLWriteCursor($this->entries);
		
		return $this->cursor;
	}

	function _loadEntries()
	{
		$this->dataLockFile->acquireRead();
		$lock = $this->dataLockFile->getHandle();
		
		$modified = fread($lock, 20);
		if($this->data_load === NULL || $this->data_load < $modified)
		{
			$entries = NULL;
			$this->data_load = $modified;

			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();

			$line = fgets($dataHandle);
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->dataFile->releaseRead();
				$this->dataLockFile->releaseRead();
				return NULL;
			}
	
			$num_entries = rtrim($matches[1]);

			if($num_entries != 0)
			{
				$columns = array_keys($this->getColumns());
				$skip = false;
				$entries = array();
	
				for($i = 0;  $i < $num_entries; $i++) {
					$line = rtrim(fgets($dataHandle, 4096));

					if(!$skip) {
						if(preg_match('/^(\d+):(.*)$/', $line, $matches))
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
				
					preg_match_all("#(-?\d+(?:\.\d+)?|'.*?(?<!\\\\)'|NULL);#s", $data, $matches);
					for($m = 0; $m < count($matches[0]); $m++) {
						if($matches[1][$m] == 'NULL')
							$entries[$row][$m] = NULL;
						else
							$entries[$row][$m] = $matches[1][$m];
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
		
		if($this->data_load === NULL || $this->data_load >= $modified)
		{
			$toprint = count($this->entries)."\r\n";
			foreach($this->entries as $number => $entry) {
				$toprint .= $number.': ';
				foreach($entry as $key => $value) {
					if($value === NULL)
						$value = 'NULL';
					
					if(is_string($value))
						$toprint .= "'$value';";
					else
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

	function drop()
	{
		if($this->lock !== null)
		{
			unlink($this->columnsFile->getPath());
			unlink($this->columnsLockFile->getPath());
			unlink($this->dataFile->getPath());
			unlink($this->dataLockFile->getPath());
			return true;
		}
		else
			return false;
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
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = NULL;
		
		if(!$temporary) {
			$table = new fSQLStandardTable($table_name, $this);
		} else {
			$table = new fSQLTemporaryTable($table_name, $this);
			$this->loadedTables[$table_name] =& $table;
		}

		$table->create($columns);
		
		return $table;
	}

	function getPath()
	{
		return $this->path_to_db;
	}
	
	function &getTable($table_name)
	{
		if(!isset($this->loadedTables[$table_name])) {
			$table = new fSQLStandardTable($table_name, $this);
			$this->loadedTables[$table_name] = $table;
			unset($table);
		}
		
		return $this->loadedTables[$table_name];
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * 
	 * @return array the table names
	 */
	function listTables()
	{
		$dir = opendir($this->path_to_db);

		$tables = array();
		while (false !== ($file = readdir($dir))) {
			if ($file !== '.' && $file !== '..' && !is_dir($file)) {
				if(substr($file, -12) == '.columns.cgi') {
					$tables[] = substr($file, 0, -12);
				}
			}
		}
		
		closedir($dir);
		
		return $tables;
	}
	
	function loadTable($table_name)
	{		
		$table = $this->getTable($table_name);
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
			if(!$oldTable->temporary()) {
				$newTable =& $new_db->createTable($oldTable->getColumns());
				copy($oldTable->dataFile->getPath(), $newTable-dataFile->getPath());
				copy($oldTable->dataLockFile->getPath(), $newTable->dataLockFile->getPath());
				$this->dropTable($old_table_name);
			} else {
				$new_db->loadedTables[$new_table_name] =& $oldTable;
				$oldTable->rename($new_table_name);
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
			$table->drop();			
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

class fSQLOrderByClause
{
	var $tosort;
	
	function fSQLOrderByClause($tosort)
	{
		$this->tosort = $tosort;
	}
	
	function sort(&$data)
	{
		usort($data, array($this, '_orderBy'));
	}
	
	function _orderBy($a, $b)
	{
		foreach($this->tosort as $tosort) {
			$key = $tosort['key'];
			$ascend = $tosort['ascend'];
			
			$a_value = $a[$key];
			$b_value = $b[$key];
			
			if($ascend) {
				if ($a_value === NULL) {
					return -1;
				}
				elseif ($b_value === NULL) {
					return 1;
				}
				elseif($a_value < $b_value) {
					return -1;
				}
				elseif ($a_value > $b_value) {
					return 1;
				}
			} else {
				if ($a_value === NULL) {
					return 1;
				}
				elseif ($b_value === NULL) {
					return -1;
				}
				elseif($a_value < $b_value) {
					return 1;
				}
				elseif ($a_value > $b_value) {
					return -1;
				}
			}
		}
		return 0;
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
	var $custom_func = array('bin', 'bit_length', 'char','concat','concat_ws','count','curdate','curtime','database','dayofweek',
	   'dayofyear','elt','field','from_unixtime','last_insert_id', 'left','locate','log','log2','log10','lpad','max','min',
	   'mod','now','repeat','right','row_count','sign','space','substring_index','sum','truncate','unix_timestamp',
	   'weekday');
	var $renamed_func = array('ascii' => 'ord', 'conv'=>'base_convert','ceiling' => 'ceil','degrees'=>'rad2deg','format'=>'number_format',
	   'length'=>'strlen','lower'=>'strtolower','ln'=>'log','power'=>'pow','quote'=>'addslashes',
	   'radians'=>'deg2rad','repeat'=>'str_repeat','replace'=>'strtr','reverse'=>'strrev',
	   'rpad'=>'str_pad','sha' => 'sha1', 'substring'=>'substr','upper'=>'strtoupper');
	
	function define_db($name, $path)
	{
		$path = realpath($path);
		if(!$path || !is_dir($path)) {
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
		$this->_unlock_tables();
		
		foreach (array_keys($this->databases) as $db_name ) {
			$this->databases[$db_name]->close();
		}
		
		$this->updatedTables = array();
		$this->join_lambdas = array();
		$this->Columns = array();
		$this->databases = array();
		$this->cursors = array();
		$this->data = array();
		$this->currentDB = null;
		$this->error_msg = null;
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
		$this->error_msg = $error.'\r\n';
		return null;
	}
	
	function _error_table_not_exists($db_name, $table_name)
	{
		$this->error_msg = "Table {$db_name}.{$table_name} does not exist"; 
	}

	function _error_table_read_lock($db_name, $table_name)
	{
		$this->error_msg = "Table {$db_name}.{$table_name} is locked for reading only"; 
	}
	
	function _load_table(&$db, $table_name)
	{		
		$table = $db->loadTable($table_name);
		if(!$table)
			$this->_set_error("Unable to load table {$db->name}.{$table_name}");

		return $table;
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
			default:			$this->_set_error('Invalid Query');  return NULL;
		}
	}
	
	function _query_begin($query)
	{
		if(preg_match('/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is', $query, $matches)) {			
			$this->_begin();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}
	
	function _query_start($query)
	{
		if(preg_match('/\ASTART\s+TRANSACTION\s*[;]?\Z/is', $query, $matches)) {			
			$this->_begin();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}
	
	function _query_commit($query)
	{
		if(preg_match('/\ACOMMIT\s*[;]?\Z/is', $query, $matches)) {
			$this->_commit();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}
	
	function _query_rollback($query)
	{
		if(preg_match('/\AROLLBACK\s*[;]?\Z/is', $query, $matches)) {
			$this->_rollback();
			return true;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}
	}
	
	function _query_create($query)
	{
		if(preg_match('/\ACREATE(?:\s+(TEMPORARY))?\s+TABLE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*?)`?(?:\s*\((.+)\)|\s+LIKE\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*))/is', $query, $matches)) {
			
			list(, $temporary, $ifnotexists, $db_name, $table_name, $column_list) = $matches;
	
			if(!$table_name) {
				$this->_set_error('No table name specified');
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
				preg_match_all('/(?:(?:CONSTRAINT\s+(?:[A-Z][A-Z0-9\_]*\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+([A-Z][A-Z0-9\_]*))?\s*\((.+?)\))|(?:`?([A-Z][A-Z0-9\_]*?)`?(?:\s+((?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)\s*(UNSIGNED\s+)?(.*?)?(?:,|\)|$))/is', trim($column_list), $Columns);

				if(!$Columns) {
					$this->_set_error('Parsing error in CREATE TABLE query');
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
						if($keytype === 'index')
							$keytype = 'key';
						$keycolumns = explode(',', $Columns[3][$c]);
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
							$type = 's';
						} else if(in_array($type, array('BIT','TINYINT', 'SMALLINT','MEDIUMINT','INT','INTEGER','BIGINT'))) {
							$type = 'i';
						} else if(in_array($type, array('FLOAT','REAL','DOUBLE','DOUBLE PRECISION','NUMERIC','DEC','DECIMAL'))) {
							$type = 'f';
						} else {
							switch($type)
							{
								case 'DATETIME':
									$type = 'dt';
									break;
								case 'DATE':
									$type = 'd';
									break;
								case 'ENUM':
									$type = 'e';
									break;
								case 'TIME':
									$type = 't';
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
						
						if($type === 'e') {
							preg_match_all("/'.*?(?<!\\\\)'/", $Columns[6][$c], $values);
							$restraint = $values[0];
						} else {
							$restraint = NULL;
						}
				
						if(preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\2))/is", $options, $matches)) {
							$default = $matches[1];
							if(!$null && strcasecmp($default, 'NULL')) {
								if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
									if($type === 'i')
										$default = (int) $matches[2];
									else if($type === 'f')
										$default = (float) $matches[2];
									else if($type === 'e') {
										if(in_array($default, $restraint))
											$default = array_search($default, $restraint) + 1;
										else
											$default = 0;
									}
								} else {
									if($type === 'i')
										$default = (int) $default;
									else if($type === 'f')
										$default = (float) $default;
									else if($type === 'e') {
										$default = (int) $default;
										if($default < 0 || $default > count($restraint)) {
											$this->_set_error('Numeric ENUM value out of bounds');
											return NULL;
										}
									}
								}
							}
						} else if($type === 's')
							// The default for string types is the empty string 
							$default = "''";
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
		if(preg_match('/\A((INSERT|REPLACE)(?:\s+(IGNORE))?\s+INTO\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?)\s+(.+?)\s*[;]?\Z/is', $query, $matches)) { 
			list(, $beginning, $command, $ignore, $db_name, $table_name, $the_rest) = $matches;
		} else {
			$this->_set_error('Invalid Query');
			return NULL;
		}

		// INSERT...SELECT
		if(preg_match('/^SELECT\s+.+/is', $the_rest)) { 
			$id = $this->_query_select($the_rest);
			while($values = $this->fetch_array($id)) {
				$this->query_count--;
				$this->_query_insert($beginning." VALUES('".join("', '", $values)."')");
			}
			$this->free_result($id);
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
			$this->_set_error('Invalid Query');
			return NULL;
		}

		preg_match_all("/\s*(DEFAULT|AUTO|NULL|'.*?(?<!\\\\)'|(?:[\+\-]\s*)?\d+(?:\.\d+)?|[^$])\s*(?:$|,)/is", $get_data_from, $newData);
		$dataValues = $newData[1];
	
		if($check_names === 1) {
			if(count($dataValues) != count($Columns)) {
				$this->_set_error("Number of inserted values and columns not equal");
				return null;
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
					$this->_set_error("Invalid column name '{$col_name}' found");
					return NULL;
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
				$this->_set_error("Trying to insert too many values");
				return NULL;
			} else {
				$Data = $dataValues;
			}
		}
		
		$newentry = array();
		$col_index = -1;
		
		////Load Columns & Data for the Table
		foreach($tableColumns as $col_name => $columnDef)  {

			++$col_index;
			
			$data = trim($Data[$col_index]);				
			$data = strtr($data, array("$" => "\$", "\$" => "\\\$"));
			
			$isEmpty = empty($data);
			$isNull = !strcasecmp($data, 'NULL');
			
			////Check for Auto_Increment
			if(($isNull || $isEmpty || !strcasecmp($data, 'AUTO')) && $columnDef['auto'] == 1) {
				$tableCursor->last();
				$lastRow = $tableCursor->getRow();
				if($lastRow !== NULL)
					$this->insert_id = $lastRow[$col_index] + 1;
				else
					$this->insert_id = 1;
				$newentry[$col_index] = $this->insert_id;
			}
			// Blank, NULL, or DEFAULT values
			else if($isNull  || $isEmpty || !strcasecmp($data, 'DEFAULT')) {
				$newentry[$col_index] = $columnDef['default'];  // default is set to NULL if this column is not nullable
			} else if($columnDef['type'] === 'i') {
				if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $data, $matches)) {
					$newentry[$col_index] = (int) $matches[1];
				}
				else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $data)) {
					$newentry[$col_index] = (int) $data;
				}
				else {
					$this->_set_error('Invalid integer value for insert');
					return NULL;
				}
			} else if($columnDef['type'] === 'f') {
				if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $data, $matches)) {
					$newentry[$col_index] = (float) $matches[1];
				}
				else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $data)) {
					$newentry[$col_index] = (float) $data;
				}
				else {
					$this->_set_error('Invalid float value for insert');
					return NULL;
				}
			} else if($columnDef['type'] === 'e') {
				if(in_array($data, $columnDef['restraint'])) {
					$newentry[$col_index]= array_search($data, $columnDef['restraint']) + 1;
				} else if(is_numeric($data))  {
					$val = (int) $data;
					if($val >= 0 && $val <= count($columnDef['restraint']))
						$newentry[$col_index]= $val;
					else {
						$this->_set_error('Numeric ENUM value out of bounds');
						return NULL;
					}
				} else {
					$newentry[$col_index] = $columnDef['default'];
				}
			}
			else if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $data, $matches)) {
				$newentry[$col_index] = (string) $matches[1];
			} else { $newentry[$col_index] = $data; }
	
			////See if it is a PRIMARY KEY or UNIQUE
			if($columnDef['key'] === 'p' || $columnDef['key'] === 'u') {
				if($replace) {
					$tableCursor->first();
					while(!$tableCursor->isDone()) {
						$row = $tableCursor->getRow();
						if($row[$col_index] == $newentry[$col_index]) {
							$tableCursor->deleteRow();
							$this->affected++;
						}
						$tableCursor->next();
					}
				} else {
					$tableCursor->first();
					while(!$tableCursor->isDone()) {
						$row = $tableCursor->getRow();
						if($row[$col_index] == $newentry[$col_index]) {
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
		if(preg_match('/\AUPDATE\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s+SET\s+(.*)(?:\s+WHERE\s+.+)?\s*[;]?\Z/is', $query, $matches)) {
			$matches[3] = preg_replace('/(.+?)(\s+WHERE\s+)(.*)/is', '\\1', $matches[3]);
			$table_name = $matches[2];

			if(!$matches[1])
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$matches[1]];
				
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
			$columnNames = array_keys($columns);
			$cursor =& $table->getCursor();

			if(preg_match_all("/`?((?:\S+)`?\s*=\s*(?:'(?:.*?)'|\S+))`?\s*(?:,|\Z)/is", $matches[3], $sets)) {
				foreach($sets[1] as $set) {
					$s = preg_split('/`?\s*=\s*`?/', $set);
					$SET[] = $s;
					if(!isset($columns[$s[0]])) {
						$this->_set_error("Invalid column name '{$s[0]}' found");
						return null;
					}
				}
				unset($s);
			}
			else
				$SET[0] =  preg_split('/\s*=\s*/', $matches[3]);

			$where = null;
			if(preg_match('/\s+WHERE\s+((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/is', $query, $sets))
			{
				$where = $this->_build_where($sets[1], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
				if(!$where) {
					$this->_set_error('Invalid/Unsupported WHERE clause');
					return null;
				}
				$where = "return ($where);";
			}
			
			$newentry = array();
			$col_indicies = array_flip($columnNames);
			
			for( $cursor->first(); !$cursor->isDone(); $cursor->next())
			{
				$entry = $cursor->getRow();

				if($where === null || eval($where)) {
					foreach($SET as $set) {
						list($column, $value) = $set;
						
						$columnDef = $columns[$column];
						
						if(!strcasecmp($value, "NULL") || !strcasecmp($value, "DEFAULT"))
							$value = $columnDef['default'];
						else if(preg_match('/\A([A-Z][A-Z0-9\_]*)/i', $value))
							$value = $entry[$value];
						else if($columnDef['type'] === 'i') {
							if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $sets)) {
								$value = (int) $sets[1];
							}
							else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
								$value = (int) $value;
							}
							else {
								$this->_set_error('Invalid integer value for update');
								return NULL;
							}
						} else if($columnDef['type'] === 'f') {
							if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $sets)) {
								$value = (float) $sets[1];
							}
							else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
								$value = (float) $value;
							}
							else {
								$this->_set_error('Invalid float value for update');
								return NULL;
							}
						} else if($columnDef['type'] === 'e') {
							if(in_array($value, $columnDef['restraint'])) {
								$value = array_search($value, $columnDef['restraint']) + 1;
							} else if(is_numeric($value))  {
								$value = (int) $value;
								if($value < 0 || $value > count($columnDef['restraint'])) {
									$this->_set_error('Numeric ENUM value out of bounds');
									return NULL;
								}
							} else {
								$value = $columnDef['default'];
							}
						} else if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $sets)) {
							$value = (string) $sets[1];
						}
						
						$cursor->updateField($col_indicies[$column], $value);
					}
					
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
		when     matched then update set d.txt = s.txt
		when not matched then insert (id, txt) values (s.id, s.txt);
	*/
	function _query_merge($query)
	{
		if(preg_match('/\AMERGE\s+INTO\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?(?:\s+AS\s+`?([A-Z][A-Z0-9\_]*)`?)?\s+USING\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)(?:\s+AS\s+([A-Z][A-Z0-9\_]*))?\s+ON\s+(.+?)(?:\s+WHEN\s+MATCHED\s+THEN\s+(UPDATE .+?))?(?:\s+WHEN\s+NOT\s+MATCHED\s+THEN\s+(INSERT .+?))?/is', $query, $matches)) {
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
			
			if(preg_match('/(?:\()?(\S+)\s*=\s*(\S+)(?:\))?/', $on_clause, $on_pieces)) {
			
			} else {
				$this->_set_error('Invalid ON clause');
				return NULL;
			}
			
			$TABLES = explode(',', $matches[1]);
			foreach($TABLES as $table_name) {
				$table_name = trim($table_name);
				if(!$this->table_exists($table_name)) { $this->error_msg = "Table $table_name does not exist";  return NULL; }
				$table = $this->load_table($table_name);
				$tables[] = $table;
			}
			foreach($tables as $table) {
				if($table['columns'] != $tables[1]['columns']) { $this->error_msg = 'Columns in the tables to be merged do not match'; return NULL; }
				foreach($table['entries'] as $tbl_entry) { $entries[] = $tbl_entry; }
			}
			$this->print_tbl($matches[2], $tables[1]['columns'], $entries);
			return TRUE;
		} else {
			$this->_set_error('Invalid MERGE query');
			return NULL;
		}
	}
 
	////Select data from the DB
	function _query_select($query)
	{
	//	var_dump(memory_get_usage());

		$randval = rand();
		$selects = preg_split('/\s+UNION\s+/i', $query);
		$e = 0;
		foreach($selects as $select) {
			unset($matches, $where, $tables, $Columns);
			
			$simple = 1;
			$distinct = 0;
			if(preg_match('/(.+?)\s+(?:WHERE|ORDER\s+BY|LIMIT)\s+(.+?)/is',$select)) {
				$simple = 0;
				preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.+?)\s+FROM\s+(.+?)\s+(?:WHERE|ORDER\s+BY|LIMIT)\s+/is', $select, $matches);
				$matches[4] = preg_replace('/(.+?)\s+(WHERE|ORDER\s+BY|LIMIT)\s+(.+?)/is', '\\1', $matches[4]);
			}
			else if(preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*?)\s+FROM\s+(.+)/is', $select, $matches)) { /* I got the matches, do nothing else */ }
			else { preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*)/is', $select, $matches); $matches[4] = 'FSQL'; }

			$distinct = !strncasecmp($matches[1], 'DISTINCT', 8);
			$has_random = $matches[2] !== ' ';
			
			//expands the tables and loads their data
			$tbls = explode(',', $matches[4]);
			$joins = array();
			$joined_info = array( 'tables' => array(), 'offsets' => array(), 'columns' =>array() );
			foreach($tbls as $table_name) {
				if(preg_match('/\A\s*(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)(.*)/is', $table_name, $tbl_data)) {
					list(, $db_name, $table_name, $the_rest) = $tbl_data;
					if(empty($db_name)) {
						$db_name = $this->currentDB->name;
					}
					$saveas = $db_name.'.'.$table_name;

					if(preg_match('/\A\s+(?:AS\s+)?([A-Z][A-Z0-9\_]*)(.*)/is', $the_rest, $alias_data)) {
						if(!in_array(strtolower($alias_data[1]), array('natural', 'left', 'right', 'full', 'outer', 'cross', 'inner')))
							list(, $saveas, $the_rest) = $alias_data;
					}
				} else {
					$this->_set_error('Invalid table list');
					return NULL;
				}

				$db =& $this->databases[$db_name];
				
				if(!($table = $this->_load_table($db, $table_name))) {
					return NULL;
				}
			
				if(!isset($tables[$saveas]))
					$tables[$saveas] = $table;
				else
					return $this->_set_error("Table named '$saveas' already specified");

				$joins[$saveas] = array();
				$joined_info['tables'][$saveas] = $table['columns'];
				$joined_info['offsets'][$saveas] = count($joined_info['columns']);
				$joined_info['columns'] = array_merge($joined_info['columns'], array_keys($table['columns']));

				if(!empty($the_rest)) {
					preg_match_all('/((?:(?:NATURAL\s+)?(?:LEFT|RIGHT|FULL)(?:\s+OUTER)?|NATURAL|INNER|CROSS)\s+)?JOIN\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)(?:\s+(?:AS\s+)?([A-Z][A-Z0-9\_]*)(?=\s*\Z|\s+(?:USING|ON|INNER|NATURAL|CROSS|LEFT|RIGHT|FULL|JOIN)))?(?:\s+(USING|ON)\s*(?:(?:\((.*?)\))|(?:(?:\()?((?:\S+)\s*=\s*(?:\S+)(?:\))?))))?/is', $the_rest, $join);
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

						$join_db_name = $join[2][$i];
						$join_table_name = $join[3][$i];
						$join_table_alias = $join[4][$i];

						if(empty($join_db_name))
							$join_db =& $this->currentDB;
						else
							$join_db =& $this->databases[$join_db_name];

						if(empty($join_table_alias))
							$join_table_alias = $join_table_name;

						if(!($join_table = $this->_load_table($join_db, $join_table_name))) {
							return NULL;
						}

						if(!isset($tables[$join_table_alias]))
							$tables[$join_table_alias] = $join_table;
						else
							return $this->_set_error("Table named '$join_table_alias' already specified");
						
						$join_table_columns = array_keys($join_table['columns']);

						$clause = strtoupper($join[5][$i]);
						if($clause === 'USING' || !$clause && $is_natural) {
							if($clause)   // USING
								$shared_columns = preg_split('/\s*,\s*/', trim($join[6][$i]));
							else  // NATURAL
								$shared_columns = array_intersect($joined_info['columns'], $join_table_columns);
							
							$conditional = '';
							foreach($shared_columns as $shared_column) {
								$conditional .= " AND {{left}}.$shared_column=$join_table_alias.$shared_column";
							}
							$conditions = substr($conditional, 5);
						}
						else if($clause === 'ON') {
							$conditions = trim($join[6][$i]);
						}

						$joined_info['tables'][$join_table_alias] = $join_table['columns'];
						$new_offset = count($joined_info['columns']);
						$joined_info['columns'] = array_merge($joined_info['columns'], $join_table_columns);

						$conditional = $this->_build_where($conditions, $joined_info, true);
						if(!$conditional) {
							$this->_set_error('Invalid/Unsupported WHERE clause');
							return null;
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
				$base_table = $tables[$base_table_name];
				$join_columns_size = count($base_table['columns']);
				$join_data = $base_table['entries'];
				foreach($join_ops as $join_op) {
					$joining_table = $tables[$join_op['table']];
					$joining_columns_size = count($joining_table['columns']);

					switch($join_op['type'])
					{
						default:
							$join_data = $this->_inner_join($join_data, $joining_table['entries'], $join_op['comparator']);
							break;
						case FSQL_JOIN_LEFT:
							$join_data = $this->_left_join($join_data, $joining_table['entries'], $join_op['comparator'], $joining_columns_size);
							break;
						case FSQL_JOIN_RIGHT:
							$join_data = $this->_right_join($join_data, $joining_table['entries'], $join_op['comparator'], $join_columns_size);
							break;
						case FSQL_JOIN_FULL:
							$join_data = $this->_full_join($join_data, $joining_table['entries'], $join_op['comparator'], $join_columns_size, $joining_columns_size);
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
			
			preg_match_all("/(?:\A|\s*)((?:(?:-?\d+(?:\.\d+)?)|(?:[A-Z][A-Z0-9\_]*\s*\(.*?\))|(?:(?:(?:[A-Z][A-Z0-9\_]*)\.)?(?:(?:[A-Z][A-Z0-9\_]*)|\*)))(?:\s+(?:AS\s+)?[A-Z][A-Z0-9\_]*)?)\s*(?:\Z|,)/is", trim($matches[3]), $Columns);
			
			$ColumnList = array();
			$selectedInfo = array();
			foreach($Columns[1] as $column) {		
				if(preg_match('/\A((?:[A-Z][A-Z0-9\_]*)\s*\((?:.+?)?\))(?:\s+(?:AS\s+)?([A-Z][A-Z0-9\_]*))?\Z/is', $column, $colmatches)) {
					list(, $function_call, $alias, ) = $colmatches;
					if(empty($alias))
						$alias = $function_call;
					$ColumnList[] = $alias;
					$selectedInfo[] = array('function', $function_call);
				}
				else if(preg_match('/\A(?:([A-Z][A-Z0-9\_]*)\.)?((?:[A-Z][A-Z0-9\_]*)|\*)(?:\s+(?:AS\s+)?([A-Z][A-Z0-9\_]*))?\Z/is',$column, $colmatches)) {
					list(, $table_name, $column) = $colmatches;
					if($column === '*') {
						if(isset($colmatches[3])) 
							return $this->_set_error('Unexpected alias after "*"');

						if(!empty($table_name)) {
							$start_index = $joined_info['offsets'][$table_name];
							$column_names = array_keys($tables[$table_name]['columns']);
							$ColumnList = array_merge($ColumnList, $column_names);
							foreach($column_names as $index => $column_name) {
								$selectedInfo[] = array('column', $start_index + $index);
							}
						} else {
							foreach($tables as $tname => $tabledata) {
								$start_index = $joined_info['offsets'][$tname];
								$column_names = array_keys($tabledata['columns']);
								$ColumnList = array_merge($ColumnList, $column_names);
								foreach($column_names as $index => $column_name) {
									$selectedInfo[] = array('column', $start_index + $index);
								}
							}
						}
					} else {
						if($table_name) {
							$index = array_search($column, array_keys($tables[$table_name]['columns'])) + $joined_info['offsets'][$table_name];
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
				else if(preg_match("/\A(-?\d+(?:\.\d+)?)(?:\s+(?:AS\s+)?([A-Z][A-Z0-9\_]*))?\Z/is", $column, $colmatches)) {
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

			$this_random = array();
			$limit = null;
			$tosort = array();
			$where = null;
			
			if($matches[4] !== 'FSQL') {
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
						if(preg_match('/([A-Z][A-Z0-9\_]*)(?:\s+(ASC|DESC))?/is', $ORDERBY[$i], $additional)) {
							$column_name = $additional[1];
							$index = array_search($column_name, $ColumnList);
							if(empty($additional[2])) { $additional[2] = 'ASC'; }
							$tosort[] = array('key' => $index, 'ascend' => !strcasecmp("ASC", $additional[2]));
						}
					}
				}
				
				if(preg_match('/\s+WHERE\s+((?:.+)(?:(?:((?:\s+)(?:AND|OR)(?:\s+))?(?:.+)?)*)?)(?:\s+(?:ORDER\s+BY|LIMIT))?/is', $select, $first_where)) {
					$where = $this->_build_where($first_where[1], $joined_info);
					if(!$where) {
						$this->_set_error('Invalid/Unsupported WHERE clause');
						return null;
					}
					//var_dump($where);
				}
			}
			//else { $data[$e++] = $entry; }

/*
			if(!empty($data) && $has_random && preg_match('/ RANDOM(?:\((\d+)\)?)?\s+/is', $select, $additional)) {
				if(!$additional[1]) { $additional[1] = 1; }
				if($additional[1] >= count($this_random)) { $results = $data; }
				else {
					$random = array_rand($this_random, $additional[1]);
					if(is_array($random)) {	for($i = 0; $i < count($random); ++$i) { $results[] = $data[$random[$i]]; }	}
					else { $results[] = $data[$random]; }
				}
				unset($data);
				$data = $results;
			} */
		
			$line = "";
			foreach($selectedInfo as $info) {
				//$value = strtr($value, array("\\\"" => "\"", "\\\\\"" => "\\\""));
				if($info[0] === 'column') {
					$line .= '$entry[' . $info[1] .'], ';
				}
				else if($info[0] === 'number') {
					$line .= $info[1].', ';
				}
				else if($info[0] === 'function') {
					$expr = $this->_build_expr($info[1], $joined_info, false);
					$line .= $expr['expression'].', ';
				}
			}
			
			$line = '$final_set[] = array('. substr($line, 0, -2) . ');';
			if($where !== null)
				$line = "if($where) {\r\n$line\r\n}";
			
			$final_set = array();
			$code = <<<EOT
			foreach(\$data as \$entry) {
				$line
			}
EOT;

			echo $code;
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

		$this->Columns[$randval] = array_values($ColumnList);
		$this->cursors[$randval] = array(0, 0);
		$this->data[$randval] = $final_set;

	//	var_dump(memory_get_usage());

		return $randval;
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
			//	$right_entry = array_values($right_entry);
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

	function _load_functions($section, $entry, $newentry) {
		if(preg_match('/(.+?)\((.+?)?\)\s+AS\s+([A-Z][A-Z0-9\_]*)/is',$section,$functions)) {
			$in_a_class = 0;
			$is_grouping = 0;
			$function = strtolower($functions[1]);

			if(isset($this->renamed_func[$function])) {
				$function = $this->renamed_func[$function];
			} else if(in_array($function, $this->custom_func)) {
				$in_a_class = 1;
				if(in_array($function, array('count', 'max', 'min', 'sum')))
					$is_grouping = 1;
				$function = '_fsql_functions_'.$function;
			} else if(!in_array($function, $this->allow_func)) {
				$this->_set_error('Call to unknown SQL function');
				continue;
			}
			
			if(!empty($functions[2])) {
				$parameter = explode(',', $functions[2]);
				foreach($parameter as $param) {
					if(!preg_match("/'(.+?)'/is", $param) && !is_numeric($param) && !$is_grouping) {
						if(preg_match('/(?:\S+)\.(?:\S+)/', $param)) { list($name, $var) = explode('.', $param); }
						else { $var = $param; }
						$parameters[] = $entry[$var];
					}
					else { $parameters[] = $param; }
					if($is_grouping) {
						$parameters[] = $table;
					}
				}
				if($in_a_class == 0) { $newentry[$functions[3]] = call_user_func_array($function, $parameters); }
				else { $newentry[$functions[3]] = call_user_func_array(array($this,$function), $parameters); }
			}
			else {
				if($in_a_class == 0) { $newentry[$functions[3]] = call_user_func($function); }
				else { $newentry[$functions[3]] = call_user_func(array($this,$function)); }
			}
			return $newentry;
		}
		return NULL;
	}

	function _build_where($statement, $join_info, $isOnClause = false)
	{
		if($statement) {
			preg_match_all("/(\A\s*|\s+(?:AND|OR)\s+)(\S+?)\s*(!=|<>|>=|<=>?|>|<|=|IS(?:\s+NOT)?|(?:NOT\s+)?IN|(?:NOT\s+)?R?LIKE|(?:NOT\s+)?REGEXP)\s*('.*?'|\S+)/is", $statement, $WHERE);
			
			$where_count = count($WHERE[0]);
			if($where_count === 0)
				return null;
			
			$condition = "";
			
			for($i = 0; $i < $where_count; ++$i) {				
				$logicalOp = trim($WHERE[1][$i]);
				$leftStr = $WHERE[2][$i];
				$operator = preg_replace("/\s+/", " ", strtoupper($WHERE[3][$i]));
				$rightStr = $WHERE[4][$i];

				$trueValue = FSQL_TRUE;
				$falseValue = FSQL_FALSE;
				
				if(!strcasecmp($logicalOp, 'AND'))
					$logicalOp = '&';
				else if(!strcasecmp($logicalOp, 'OR'))
					$logicalOp = '|';
				else
					$logicalOp = null;
				
				if($operator === '<>') { $operator = '!='; }
				
				$left = $this->_build_expr($leftStr, $join_info, $isOnClause);
				$right = $this->_build_expr($rightStr, $join_info, $isOnClause);

				$leftExpr = $left['expression'];
				$rightExpr = $right['expression'];

				if($logicalOp !== null) 
					$condition .= " $logicalOp ";

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
							$condition .= "fSQLEnvironment::_{$nullcheck}_eq($leftExpr, $rightExpr)";
						else
							$condition .= "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case '!=':
						if($nullcheck)
							$condition .= "fSQLEnvironment::_{$nullcheck}_ne($leftExpr, $rightExpr)";
						else
							$condition .= "(($leftExpr != $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case '>':
						if($nullcheck)
							$condition .= "fSQLEnvironment::_{$nullcheck}_gt($leftExpr, $rightExpr)";
						else
							$condition .= "(($leftExpr > $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case '>=':
						if($nullcheck)
							$condition .= "fSQLEnvironment::_{$nullcheck}_ge($leftExpr, $rightExpr)";
						else
							$condition .= "(($leftExpr >= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case '<':
						if($nullcheck)
							$condition .= "fSQLEnvironment::_{$nullcheck}_lt($leftExpr, $rightExpr)";
						else
							$condition .= "(($leftExpr < $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case '<=':
						if($nullcheck)
							$condition .= "fSQLEnvironment::_{$nullcheck}_le($leftExpr, $rightExpr)";
						else
							$condition .= "(($leftExpr <= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case '<=>':
						$condition .= "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
						break;
					case 'IS NOT':
						$trueValue = FSQL_FALSE;
						$falseValue = FSQL_TRUE;
					case 'IS':
						if($rightExpr === 'NULL')
							$condition .= "($leftExpr === NULL ? $trueValue : $falseValue)";
						else if($rightExpr === 'TRUE')
							$condition .= "($leftExpr == TRUE ? $trueValue : $falseValue)";
						else if($rightExpr === 'FALSE')
							$condition .= "(in_array($leftExpr, array(0, 0.0, ''), true) ? $trueValue : $falseValue)";
						else
							return null;
						break;
					case 'LIKE':
						$condition .= "fSQLEnvironment::_fsql_like($leftExpr, $rightExpr)";
						break;
					case 'NOT RLIKE':
					case 'NOT REGEXP':
						$trueValue = FSQL_FALSE;
						$falseValue = FSQL_TRUE;
					case 'RLIKE':
					case 'REGEXP':
						$condition .= "fSQLEnvironment::_fsql_regexp($leftExpr, $rightExpr)";
						break;
					default:
						$condition .= "$leftExpr $operator $rightExpr";
						break;
				}
			}
			return "($condition) === ".FSQL_TRUE;
		}
		return NULL;
	}
 
	function _build_expr($exprStr, $join_info, $isOnClause = false)
	{
		$nullable = true;
		$expr = null;

		// function call
		if(preg_match("/\A([A-Z][A-Z0-9\_]*)\s*\((.+?)\)/is", $exprStr, $matches)) {
			$function = strtolower($matches[1]);
			$params = $matches[2];
			$final_param_list = "";
			$in_a_class = false;

			if(isset($this->renamed_func[$function])) {
				$function = $this->renamed_func[$function];
			} else if(in_array($function, $this->custom_func)) {
				$in_a_class = true;
				if(in_array($function, array('count', 'max', 'min', 'sum')))
					$is_grouping = 1;
				$function = '_fsql_functions_'.$function;
			} else if(!in_array($function, $this->allow_func)) {
				$this->_set_error('Call to unknown SQL function');
				return null;
			}

			if(!empty($params)) {
				$paramExprs = array();
				$parameter = explode(',', $params);
				foreach($parameter as $param) {
					$paramExpr = $this->_build_expr(trim($param), $join_info, $isOnClause);
					$paramExprs[] = $paramExpr['expression'];
				}
				$final_param_list = implode(",", $paramExprs);
			}

			if($in_a_class == false) { $expr = "$function($final_param_list)"; }
			else { $expr = "\$this->$function($final_param_list)"; }
		}
		// column/alias/keyword
		else if(preg_match("/\A(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)\Z/is", $exprStr, $matches)) {
			list( , $table_name, $column) =  $matches;
			// table.column
			if($table_name) {
				if(isset($join_info['tables'][$table_name])) {
					$table_columns = $join_info['tables'][$table_name];
					if(isset($table_columns[ $column ])) {
						$nullable = $table_columns[ $column ]['null'] == 1;
						if( isset($join_info['offsets'][$table_name]) ) {
							$colIndex = array_search($column,  $join_info['columns']) + $join_info['offsets'][$table_name];
							$expr = $isOnClause === true ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
						} else {
							$colIndex = array_search($column, array_keys($table_columns));
							$expr = "\$right_entry[$colIndex]";
						}
					}
				}
			}
			// null/unknown
			else if(!strcasecmp($exprStr, "NULL")  || !strcasecmp($exprStr, "UNKNOWN")) {
				$expr = "NULL";
			}
			// true/false
			else if(!strcasecmp($exprStr, "TRUE") || !strcasecmp($exprStr, "FALSE")) {
				$expr = strtoupper($exprStr);
				$nullable = false;
			}
			else {  // column/alias
				$colIndex = array_search($column, $join_info['columns']);
				$owner_table_name = null;
				foreach($join_info['tables'] as $join_table_name => $join_table)
				{
					if($colIndex >= $join_info['offsets'][$join_table_name])
						$owner_table_name = $join_table_name;
					else
						break;
				}
				$nullable = $join_info['tables'][$owner_table_name][$column]['null'] == 1;
				$expr = $isOnClause === true ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
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
		else if($isOnClause && preg_match("/\A{{left}}\.([A-Z][A-Z0-9\_]*)/is", $exprStr, $matches)) {
			if(($colIndex = array_search($matches[1], $join_info['columns']))) {
				$expr = "\$left_entry[$colIndex]";
			}
		}
		else
			return null;
		
		return array('nullable' => $nullable, 'expression' => $expr);
	}
	
	function _where_functions($statement, $entry = NULL, $tname = NULL)
	{
		$operator = $statement['operator'];	
		foreach($statement as $name => $section) {
			if($name === 'table' && $section && $section !== $tname) { return NULL; }
			else if($name === 'table' || $name === 'next' || $name === 'operator') { continue; }

			if(preg_match('/(.+?)\((.+?)?\)/is',$section,$functions)) {
				$in_a_class = 0;
				$function = strtolower($functions[1]);
				$parameters = array();
				
				if(isset($this->renamed_func[$function])) {
					$function = $this->renamed_func[$function];
				} else if(in_array($function, $this->custom_func)) {
					$in_a_class = 1;
					$function = '_fsql_functions_'.$function;
				} else if(!in_array($function, $this->allow_func)) {
					$this->_set_error('Call to unknown SQL function');
					continue;
				}
				
				if(!empty($functions[2])) {
					$parameter = explode(',', $functions[2]);
					foreach($parameter as $param) {
						$param = trim($param);
						if(!preg_match("/'(.+)'/is", $param) && !is_numeric($param)) {
							if(preg_match('/(?:\S+)\.(?:\S+)/', $param)) { list( , $new_var) = explode('.', $param); }
							else { $new_var = $param; }
							$parameters[] = $entry[$new_var];
						} else {
							$parameters[] = $param;
						}
					}
					if($in_a_class == 0) { $$name = call_user_func_array($function, $parameters); }
					else { $$name = call_user_func_array(array($this,$function), $parameters); }
				} else { 
					if($in_a_class == 0) { $$name = call_user_func_array($function, $parameters); }
					else { $$name = call_user_func_array(array($this,$function), $parameters); }
				}
			}
			else if($name === 'var') {
				if(preg_match("/'(.*?)(?<!\\\\)'/is", $section, $matches))
					$var = $matches[1];
				else
					$var = $entry[$section];
			} else if($name === 'value') { $value = $section; }
		}
		if(preg_match("/'(.*?)(?<!\\\\)'/is", $var, $matches)) { $var = $matches[1]; }
		if(preg_match("/'(.*?)(?<!\\\\)'/is", $value, $matches)) { $value = $matches[1]; }
		if($operator === '=' || $operator === ' ~=~ ') { $operator = '=='; }
		
		$ops = preg_split('/\s+/', $operator);
		if($ops[0] === 'NOT') {
			$operator = $ops[1];
			$not = 1;
		} else {
			$not = 0;
		}
		
		if($operator === 'LIKE') {
			$value = preg_quote($value);
			$value = preg_replace('/(?<!\\\\)_/', '.', $value);
			$value = preg_replace('/(?<!\\\\)%/', '.*', $value);
			$value = str_replace('\\\\_', '_', $value);
			$value = str_replace('\\\\%', '%', $value);
			$return = (preg_match("/\A{$value}\Z/is", $var)) ? 1 : 0;
			$return ^= $not;
		} else if($operator === 'REGEXP' || $operator === 'RLIKE') {
			$return = (eregi($value, $var)) ? 1 : 0;
			$return ^= $not;
		}
		/*else if($operator === 'IN') {
			eval("\$return = (in_array(\$var, array$value)) ? 1 : 0;");
			$return ^= $not;
		} */
		else
			eval("\$return = (\$var $operator \$value) ? 1 : 0;");

		return $return;
	}
	
	////Delete data from the DB
	function _query_delete($query)
	{
		$this->affected  = 0;
		if(preg_match('/\ADELETE\s+FROM\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)(?:\s+(WHERE\s+.+))?\s*[;]?\Z/is', $query, $matches)) {
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
			$columnNames = array_keys($columns);

			if($cursor->isDone())
				return true;
			
			if(isset($matches[3]) && preg_match('/^WHERE\s+((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/i', $matches[3], $first_where))
			{
				$where = $this->_build_where($first_where[1], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
				if(!$where) {
					$this->_set_error('Invalid/Unsupported WHERE clause');
					return null;
				}
				$where = "return ($where);";

				$col_indicies = array_flip($columnNames);
			
				while(!$cursor->isDone()) {
					$entry = $cursor->getRow();
					var_dump($cursor);
					if(eval($where))
					{					
						$cursor->deleteRow();
						$this->affected++;
					}
					$cursor->next();
				}
			} else {
				while(!$cursor->isDone()) {
					$cursor->deleteRow();
					$this->affected++;
					$cursor->next();
				}
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
			$this->_set_error('Invalid DELETE query');
			return null;
		}
	}
 
	function _query_alter($query)
	{
		if(preg_match('/\AALTER\s+TABLE\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s+(.*)/is', $query, $matches)) {
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
			
			preg_match_all('/(?:ADD|ALTER|CHANGE|DROP|RENAME).*?(?:,|\Z)/is', trim($changes), $specs);
			for($i = 0; $i < count($specs[0]); $i++) {
				if(preg_match('/\AADD\s+(?:CONSTRAINT\s+`?[A-Z][A-Z0-9\_]*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/is', $specs[0][$i], $matches)) {
					$columnDef =& $columns[$matches[1]];
					
					foreach($columns as $name => $column) {
						if($column['key'] === 'p') {
							$this->_set_error('Primary key already exists');
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
						$default = 'NULL';
					
					if(!$columnDef['null'] && strcasecmp($default, 'NULL')) {
						if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
							if($columnDef['type'] === 'i')
								$default = (int) $matches[2];
							else if($columnDef['type'] === 'f')
								$default = (float) $matches[2];
							else if($columnDef['type'] === 'e') {
								if(in_array($default, $columnDef['restraint']))
									$default = array_search($default, $columnDef['restraint']) + 1;
								else
									$default = 0;
							}
						} else {
							if($columnDef['type'] === 'i')
								$default = (int) $default;
							else if($columnDef['type'] === 'f')
								$default = (float) $default;
							else if($columnDef['type'] === 'e') {
								$default = (int) $default;
								if($default < 0 || $default > count($columnDef['restraint'])) {
									$this->_set_error('Numeric ENUM value out of bounds');
									return NULL;
								}
							}
						}
					} else if(!$columnDef['null']) {
						if($columnDef['type'] === 's')
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
						$this->_set_error('No primary key found');
						return NULL;
					}
				}
				else if(preg_match('/\ARENAME\s+(?:TO\s+)?`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is', $specs[0][$i], $matches)) {
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
				
					return $db->renameTable($table_name, $new_table_name, $new_db);
				}
				else {
					$this->_set_error('Invalid ALTER query');
					return null;
				}
			}
		} else {
			$this->_set_error('Invalid ALTER query');
			return null;
		}
	}

	function _query_rename($query)
	{
		if(preg_match('/\ARENAME\s+TABLE\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				list($old, $new) = preg_split('/\s+TO\s+/i', trim($table));
				
				if(preg_match('/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is', $old, $table_parts)) {
					list(, $old_db_name, $old_table_name) = $table_parts;
					
					if(!$old_db_name)
						$old_db =& $this->currentDB;
					else
						$old_db =& $this->databases[$old_db_name];
				} else {
					$this->_set_error('Parse error in table listing');
					return NULL;
				}
				
				if(preg_match('/(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)/is', $new, $table_parts)) {
					list(, $new_db_name, $new_table_name) = $table_parts;
					
					if(!$new_db_name)
						$new_db =& $this->currentDB;
					else
						$new_db =& $this->databases[$new_db_name];
				} else {
					$this->_set_error('Parse error in table listing');
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
			$this->_set_error('Invalid RENAME query');
			return null;
		}
	}
	
	function _query_drop($query)
	{
		if(preg_match('/\ADROP(?:\s+(TEMPORARY))?\s+TABLE(?:\s+(IF\s+EXISTS))?\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$temporary = !empty($matches[1]);
			$ifexists = !empty($matches[2]);
			$tables = explode(',', $matches[3]);
	
			foreach($tables as $table) {
				if(preg_match('/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is', $table, $table_parts)) {
					list(, $db_name, $table_name) = $table_parts;
					
					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];
				
					$table = &$db->getTable($table_name);
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
					$this->_set_error('Parse error in table listing');
					return NULL;
				}
			}
			return TRUE;
		} else if(preg_match('/\ADROP\s+DATABASE(?:\s+(IF\s+EXISTS))?\s+`?([A-Z][A-Z0-9\_]*)`?s*[;]?\Z/is', $query, $matches)) {
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
				if(preg_match('/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is', $table, $matches)) {
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
					$this->_set_error('Parse error in table listing');
					return NULL;
				}
			}
		} else {
			$this->_set_error('Invalid TRUNCATE query');
			return NULL;
		}
		
		return true;
	}
	
	function _query_backup($query)
	{
		if(!preg_match("/\ABACKUP TABLE (.*?) TO '(.*?)'\s*[;]?\Z/is", $query, $matches)) {
			if(substr($matches[2], -1) != "/")
				$matches[2] .= '/';
			
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				if(preg_match('/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is', $table, $table_name_matches)) {
					list(, $db_name, $table_name) = $table_name_matches;
					
					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];
					
					$db->copyTable($table_name, $db->path_to_db, $matches[2]);
				} else {
					$this->_set_error('Parse error in table listing');
					return NULL;
				}
			}
		} else {
			$this->_set_error('Invalid BACKUP Query');
			return NULL;
		}
	}
	
	function _query_restore($query)
	{
		if(!preg_match("/\ARESTORE TABLE (.*?) FROM '(.*?)'\s*[;]?\s*\Z/is", $query, $matches)) {
			if(substr($matches[2], -1) !== '/')
				$matches[2] .= '/';
			
			$tables = explode(',', $matches[1]);
			foreach($tables as $table) {
				if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_name_matches)) {
					list(, $db_name, $table_name) = $table_name_matches;
					
					if(!$db_name)
						$db =& $this->currentDB;
					else
						$db =& $this->databases[$db_name];
					
					$db->copyTable($table_name, $matches[2], $db->path_to_db);
				} else {
					$this->_set_error('Parse error in table listing');
					return NULL;
				}
			}
		} else {
			$this->_set_error('Invalid RESTORE Query');
			return NULL;
		}
	}
 
	function _query_show($query)
	{
		if(preg_match('/\ASHOW\s+TABLES(?:\s+FROM\s+`?([A-Z][A-Z0-9\_]*)`?)?\s*[;]?\s*\Z/is', $query, $matches)) {
			
			$randval = rand();
			
			if(!$matches[1])
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$matches[1]];
		
			$tables = $db->listTables();
			$data = array();
			
			foreach($tables as $table_name) {
				$table_name = '\''.$table_name.'\'';
				$data[] = array('name' => $table_name);
			}
			
			$this->Columns[$randval] = array('name');
			$this->cursors[$randval] = array(0, 0);
			$this->data[$randval] = $data;
		
			return $randval;
		} else if(preg_match('/\ASHOW\s+DATABASES\s*[;]?\s*\Z/is', $query, $matches)) {
			$randval = rand();
			
			$dbs = array_keys($this->databases);
			foreach($dbs as $db) {
				$db = '\''.$db.'\'';
				$data[] = array('name' => $db);
			}
			
			$this->Columns[$randval] = array('name');
			$this->cursors[$randval] = array(0, 0);
			$this->data[$randval] = $data;
		
			return $randval;
		} else {
			$this->_set_error('Invalid SHOW query');
			return NULL;
		}
	}
	
	function _query_describe($query)
	{
		if(preg_match('/\ADESC(?:RIBE)?\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s*[;]?\s*\Z/is', $query, $matches)) {
			
			$randval = rand();
			
			if(!$matches[1])
				$db =& $this->currentDB;
			else
				$db =& $this->databases[$matches[1]];
		
			$tableObj =& $db->getTable($matches[2]);
			if(!$tableObj->exists()) {
				$this->_error_table_not_exists($db->name, $matches[2]);
				return NULL;
			}
			$columns =  $tableObj->getColumns();
			
			$data = array();
			
			foreach($columns as $name => $column) {
				$name = '\''.$name.'\'';
				$null = ($column['null']) ? "'YES'" : "''";
				$extra = ($column['auto']) ? "'auto_increment'" : "''";
				
				if($column['key'] === 'p')
					$key = "'PRI'";
				else if($column['key'] === 'u')
					$key = "'UNI'";
				else
					$key = "''";

				$data[] = array('Field' => $name, 'Type' => "''", 'Null' => $null, 'Default' => $column['default'], 'Key' => $key, 'Extra' => $extra);
			}
			
			$this->Columns[$randval] = array_keys($data);
			$this->cursors[$randval] = array(0, 0);
			$this->data[$randval] = $data;
		
			return $randval;
		} else {
			$this->_set_error('Invalid DESCRIBE query');
			return NULL;
		}
	}
	
	function _query_use($query)
	{
		if(preg_match('/\AUSE\s+`?([A-Z][A-Z0-9\_]*)`?\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->select_db($matches[1]);
			return TRUE;
		} else {
			$this->_set_error('Invalid USE query');
			return NULL;
		}
	}

	function _query_lock($query)
	{
		if(preg_match('/\ALOCK\s+TABLES\s+(.+?)\s*[;]?\s*\Z/is', $query, $matches)) {
			preg_match_all('/(?:`?([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s+((?:READ(?:\s+LOCAL)?)|((?:LOW\s+PRIORITY\s+)?WRITE))/is', $matches[1], $rules);
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

				if(!strncasecmp($rules[3][$r], 'READ', 4)) {
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
		if(preg_match('/\AUNLOCK\s+TABLES\s*[;]?\s*\Z/is', $query)) {
			$this->_unlock_tables();
			return TRUE;
		} else {
			$this->_set_error('Invalid UNLOCK query');
			return NULL;
		}
	}
	
	function fetch_array($id, $type = 1)
	{
		if(!$id || !isset($this->cursors[$id]) || !isset($this->data[$id][$this->cursors[$id][0]]))
			return NULL;
		
		$entry = $this->data[$id][$this->cursors[$id][0]];
		if(!$entry)
			return NULL;
	
		$this->cursors[$id][0]++;

		if($type === FSQL_ASSOC) {  return array_combine($this->Columns[$id], $entry); }
		else if($type === FSQL_NUM) { return $entry; }
		else{ return array_merge($entry, array_combine($this->Columns[$id], $entry)); } 
	}
	
	function fetch_assoc($results) { return $this->fetch_array($results, FSQL_ASSOC); }
	function fetch_row	($results) { return $this->fetch_array($results, FSQL_NUM); }
	function fetch_both	($results) { return $this->fetch_array($results, FSQL_BOTH); }
 
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
			$this->_set_error('Bad results id passed in');
			return false;
		} else {
			$this->cursors[$id][0] = $i;
			return true;
		}
	}
	
	function num_fields($id)
	{
		if(!$id || !isset($this->Columns[$id])) {
			$this->_set_error('Bad results id passed in');
			return false;
		} else {
			return count($this->Columns[$id]);
		}
	}
	
	function fetch_field($id, $i = NULL)
	{
		if(!$id || !isset($this->Columns[$id]) || !isset($this->cursors[$id][1])) {
			$this->_set_error('Bad results id passed in');
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
	
	function _fsql_regexp($left, $right)
	{
		if($left !== null && $right !== null)
			return (eregi($right, $left)) ? FSQL_TRUE : FSQL_FALSE;
		else
			return FSQL_UNKNOWN;
	}


	function _nullcheck_eq($left, $right)
	{
		return ($left !== null && $right !== null) ? (($left == $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_ne($left, $right)
	{
		return ($left !== null && $right !== null) ? (($left != $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_lt($left, $right)
	{
		return ($left !== null && $right !== null) ? (($left < $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_le($left, $right)
	{
		return ($left !== null && $right !== null) ? (($left <= $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_gt($left, $right)
	{
		return ($left !== null && $right !== null) ? (($left > $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_ge($left, $right)
	{
		return ($left !== null && $right !== null) ? (($left >= $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}



	function _nullcheck_left_eq($left, $right)
	{
		var_dump($left, $right);
		return ($left !== null) ? (($left == $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_left_ne($left, $right)
	{
		return ($left !== null) ? (($left != $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_left_lt($left, $right)
	{
		return ($left !== null) ? (($left < $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_left_le($left, $right)
	{
		return ($left !== null) ? (($left <= $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_left_gt($left, $right)
	{
		return ($left !== null) ? (($left > $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_left_ge($left, $right)
	{
		return ($left !== null) ? (($left >= $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	


	function _nullcheck_right_eq($left, $right)
	{
		return ($right !== null) ? (($left == $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_right_ne($left, $right)
	{
		return ($right !== null) ? (($left != $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_right_lt($left, $right)
	{
		return ($right !== null) ? (($left < $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_right_le($left, $right)
	{
		return ($right !== null) ? (($left <= $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_right_gt($left, $right)
	{
		return ($right !== null) ? (($left > $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}
	
	function _nullcheck_right_ge($left, $right)
	{
		return ($right !== null) ? (($left >= $right) ? FSQL_TRUE : FSQL_FALSE) : FSQL_UNKNOWN;
	}



	function _fsql_strip_stringtags($string)
	{
		return preg_replace("/^'(.+)'$/s", "\\1", $string);
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
		list($integer, $decimals) = explode('.', $number);
		if($places == 0) { return $integer; }
		else if($places > 0) { return $integer.'.'.substr($decimals,0,$places); }
		else {   return substr($number,0,$places) * pow(10, abs($places));  }
	}
	 
	 /////Grouping and other Misc. Functions
	function _fsql_functions_count($column, $data) {
		if($column == '*') { return count($data['entries']); }
		else {   $i = 0;   foreach($data['entries'] as $entry) {  if($entry[$column]) { $i++; } }  return $i;  }
	}
	function _fsql_functions_max($column, $data) {
		foreach($data['entries'] as $entry){   if($entry[$column] > $i || !$i) { $i = $entry[$column]; }  }	return $i;
	}
	function _fsql_functions_min($column, $data) {
		foreach($data['entries'] as $entry){   if($entry[$column] < $i || !$i) { $i = $entry[$column]; }  }	return $i;
	}
	function _fsql_functions_sum($column, $data) {  foreach($data['entries'] as $entry){ $i += $entry[$column]; }  return $i; }
	 
	 /////String Functions
	function _fsql_functions_bin($string) {
		return decbin($string);
	}
	function _fsql_functions_bit_length($string) {
		return strlen($string) << 3;
	}
	function _fsql_functions_char($string) {
		$ret = '';
		$numargs = func_num_args();
		for($i = 0; $i < $numargs; ++$i) { $return[] = chr(func_get_arg($i));  }
		return implode($string, $return);
	}
	function _fsql_functions_concat_ws($string) {
		$numargs = func_num_args();
		if($numargs > 2) {
			for($i = 1; $i < $numargs; $i++) { $return[] = func_get_arg($i);  }
			return implode($string, $return);
		}
		else { return NULL; }
	}
	function _fsql_functions_concat() { return call_user_func_array(array($this,'_fsql_functions_concat_ws'), array('',func_get_args())); }
	function _fsql_functions_elt() {
		$return = func_get_arg(0);
		if(func_num_args() > 1 && $return >= 1 && $return <= func_num_args()) {	return func_get_arg($return);  }
		else { return NULL; }
	}
	function _fsql_functions_field() {
		$numargs = func_num_args();
		$args = func_get_args();
		$find = array_shift($args);
		$index = array_search($find, $args);
		if($index !== false)
			return $index + 1;
		else
			return 0;
	}
	function _fsql_functions_locate($string, $find, $start = NULL) {
		if($start) { $string = substr($string, $start); }
		$pos = strpos($string, $find);
		if($pos === false) { return 0; } else { return $pos; }
	}
	function _fsql_functions_lpad($string, $length, $pad) { return str_pad($string, $length, $pad, STR_PAD_LEFT); }
	function _fsql_functions_left($string, $end)	{ return substr($string, 0, $end); }
	function _fsql_functions_right($string,$end)	{ return substr($string, -$end); }
	function _fsql_functions_space($number)	{ return str_repeat(' ', $number); }
	function _fsql_functions_substring_index($string, $delim, $count) {
		$parts = explode($delim, $string);
		if($count < 0) {   for($i = $count; $i > 0; $i++) { $part = count($parts) + $i; $array[] = $parts[$part]; }  }
		else { for($i = 0; $i < $count; $i++) { $array[] = $parts[$i]; }  }
		return implode($delim, $array);
	}
	 
	////Date/Time functions
	function _fsql_functions_now()		{ return $this->_fsql_functions_from_unixtime(time()); }
	function _fsql_functions_curdate()	{ return $this->from_unixtime(time(), '%Y-%m-%d'); }
	function _fsql_functions_curtime() 	{ return $this->from_unixtime(time(), '%H:%M:%S'); }
	function _fsql_functions_dayofweek($date) 	{ return $this->_fsql_functions_from_unixtime($date, '%w'); }
	function _fsql_functions_weekday($date)		{ return $this->_fsql_functions_from_unixtime($date, '%u'); }
	function _fsql_functions_dayofyear($date)		{ return round($this->_fsql_functions_from_unixtime($date, '%j')); }
	function _fsql_functions_unix_timestamp($date = NULL) {
		if(!$date) { return NULL; } else { return strtotime(str_replace('-','/',$date)); }
	}
	function _fsql_functions_from_unixtime($timestamp, $format = '%Y-%m-%d %H:%M:%S')
	{
		if(!is_int($timestamp)) { $timestamp = $this->_fsql_functions_unix_timestamp($timestamp); }
		return strftime($format, $timestamp);
	}
 
}

?>