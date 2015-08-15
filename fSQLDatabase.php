<?php

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

	function close() {
		unset($this->entries, $this->num_rows, $this->pos);
	}

	function first()
	{
		$this->pos = 0;
		return $this->pos;
	}

	function getPosition()
	{
		return $this->pos;
	}

	function getRow()
	{
		if($this->pos >= 0 && $this->pos < $this->num_rows)
			return $this->entries[$this->pos];
		else
			return false;
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
		$this->pos--;
	}

	function next()
	{
		$this->pos++;
		return $this->pos;
	}

	function seek($pos)
	{
		if($pos >=0 & $pos < count($this->entries))
			$this->pos = $pos;
	}
}

class fSQLTable
{
	var $cursor = null;
	var $columns = null;
	var $entries = null;
	var $data_load =0;
	var $uncommited = false;

	function &create($path_to_db, $table_name, $columnDefs)
	{
		$table =& new fSQLTable;
		$table->columns = $columnDefs;
		$table->temporary = true;
		return $table;
	}

	function close()
	{
		if($this->cursor !== null) {
			$this->cursor->close();
		}
		unset($this->cursor, $this->columns, $this->entries, $this->data_load, $this->uncommited);
	}

	function exists() {
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

	function getEntries()
	{
		return $this->entries;
	}

	function &getCursor()
	{
		if($this->cursor === null)
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
	var $cursor = null;
	var $columns = null;
	var $entries = null;
	var $columns_path;
	var $data_path;
	var $columns_load = null;
	var $data_load = null;
	var $uncommited = false;
	var $columnsLockFile;
	var $columnsFile;
	var $dataLockFile;
	var $dataFile;
	var $lock = null;

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

	function close()
	{
		unset($this->cursor, $this->columns, $this->entries, $this->columns_path, $this->data_path,
			$this->columns_load, $this->data_load, $this->uncommited, $this->columnsLockFile, $this->dataLockFile,
			$this->dataFile, $this->lock);
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
			$default = $column['default'];
			if(is_string($default)) {
				$default = "'$default'";
			}

			if(!is_array($column['restraint'])) {
				$toprint .= $name.": ".$column['type'].";;".$column['auto'].";".$default.";".$column['key'].";".$column['null'].";\r\n";
			} else {
				$toprint .= $name.": ".$column['type'].";".implode(",", $column['restraint']).";".$column['auto'].";".$default.";".$column['key'].";".$column['null'].";\r\n";
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
		if($this->columns_load === null || strcmp($this->columns_load, $modified) < 0)
		{
			$this->columns_load = $modified;

			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();

			$line = fgets($columnsHandle);
			if(!preg_match("/^(\d+)/", $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return false;
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
					else if($default{0} == "'" && substr($default, -1) == "'") {
						$default = substr($default, 1, -1);
					}
					$this->columns[$matches[1]] = array(
						'type' => $type, 'auto' => $matches[4], 'default' => $default, 'key' => $matches[6], 'null' => $matches[7]
					);
					if(preg_match_all("/'(.*?(?<!\\\\))'/", $matches[3], $restraint) !== false) {
						$this->columns[$matches[1]]['restraint'] = $restraint[1];
					} else {
						$this->columns[$matches[1]]['restraint'] = array();
					}
				} else {
					return false;
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

		if($this->cursor === null)
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
		if($this->data_load === null || strcmp($this->data_load, $modified) < 0)
		{
			$entries = null;
			$this->data_load = $modified;

			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();

			$line = fgets($dataHandle);
			if(!preg_match("/^(\d+)/", $line, $matches))
			{
				$this->dataFile->releaseRead();
				$this->dataLockFile->releaseRead();
				return false;
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
							$entries[$row][$m] = null;
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

		if($this->data_load === null || strcmp($this->data_load, $modified) >= 0)
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
	var $name = null;
	var $path_to_db = null;
	var $loadedTables = array();

	function close()
	{
		foreach(array_keys($this->loadedTables) as $table_name) {
			$this->loadedTables[$table_name]->close();
		}

		unset($this->name, $this->path_to_db, $this->loadedTables);
	}

	function createTable($table_name, $columns, $temporary = false)
	{
		$table = null;

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

			$table = null;
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

?>