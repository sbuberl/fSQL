<?php

class fSQLStandardDriver extends fSQLDriver
{
	function &defineDatabase(&$environment, $name, $path)
	{
		$db =& new fSQLDatabase($environment, $name, $path);
		return $db;
	}	
}

class fSQLStandardSchema extends fSQLSchema
{
	var $loadedTables = array();

	function fSQLStandardSchema(&$database, $name)
	{
		$this->name = $name;
		$this->database =& $database;
		$this->path = $name !== 'public' ? $database->getPath().$name.'/' : $database->getPath();
	}
	
	function create()
	{
		if($this->name !== 'public') {
			$path = create_directory($this->path, 'schema', $this->database->getEnvironment());
			if($path !== false) {
				$this->path = $path;
			}
			else
				return false;
		}
		
		return true;
	}
	
	function close()
	{
		parent::close();
		unset($this->loadedTables);
	}
	
	function &createTable($table_name, $columns, $temporary = false)
	{
		$table = false;
		
		if(!$temporary) {
			$table =& new fSQLStandardTable($table_name, $this);
		} else {
			$table =& new fSQLMemoryTable($table_name, $this);
			$this->loadedTables[$table_name] =& $table;
		}

		$table->create($columns);
		
		return $table;
	}
	
	function &createView($view_name, $query, $columns = null)
	{
		$table =& new fSQLStandardView($view_name, $this);
		$this->tables[$view_name] =& $table;
		$table->define($query, $columns);
		return $this->tables[$view_name];
	}
	
	function &getTable($table_name)
	{
		$table = false;
		
		if(!isset($this->loadedTables[$table_name]))
		{
			if($this->tableExists($table_name))
			{
				$path_prefix = $this->path.$table_name;
		
				if(file_exists($path_prefix.'.data.cgi')) {
					$table =& new fSQLStandardTable($table_name, $this);
				} else if(file_exists($path_prefix.'.view.cgi')) {
					$table =& new fSQLStandardView($table_name, $this);
					$table->execute();
				}
				
				$this->loadedTables[$table_name] =& $table;
				unset($table);
			}
			else
				return $table;
		}
		
		return $this->loadedTables[$table_name];
	}
	
	function tableExists($table_name)
	{
		return in_array($table_name, $this->listTables());
	}
	
	/**
	 * Returns an array of names of all the tables in the database
	 * 
	 * @return array the table names
	 */
	function listTables()
	{
		$dir = opendir($this->path);

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
	
	function renameTable($old_table_name, $new_table_name, &$new_db)
	{
		$oldTable =& $this->getTable($old_table_name);
		if($oldTable !== false) {
			if(!$oldTable->temporary()) {
				$newTable =& $new_db->createTable($oldTable->getColumns());
				copy($oldTable->dataFile->getPath(), $newTable->dataFile->getPath());
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
		if($table !== false) {
			$table->drop();			
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

class fSQLStandardTableDef extends fSQLTableDef
{
	var $columns = null;
	var $columnsFile;
	var $columnsLockFile;
	var $columnsLoad = null;
	var $lock = null;
	var $readFunction = null;
	
	function fSQLStandardTableDef($path)
	{
		$this->columnsLockFile = new fSQLFile($path.'.lock.cgi');
		$this->columnsFile = new fSQLFile($path.'.cgi');
	}
	
	function close()
	{
		$this->path = null;
		$this->columns = null;
	}
	
	function drop()
	{
		if($this->lock === null)
		{
			unlink($this->columnsFile->getPath());
			unlink($this->columnsLockFile->getPath());
			$this->close();
			return true;
		}
		else
			return false;
	}

	function getReadFunction()
	{
		if($this->readFunction === null)
			$columns = $this->getColumns();
		return $this->readFunction;
	}
	
	function _buildReadWriteFuncs()
	{
		if(empty($this->columns))
			return false;
		
		$readString = '^(\d+):\s*';
		$translate = '';
		$i = 0;
		foreach($this->columns as $col_name => $column)
		{
			$typeMatch = '';
			$translateCode = '';
			switch($column['type'])
			{
				case FSQL_TYPE_INTEGER:
				case FSQL_TYPE_ENUM:
				case FSQL_TYPE_TIMESTAMP:
				case FSQL_TYPE_YEAR:
					$typeMatch = '-?\d+';
					$translateCode =  "((int) \$entry[$i])";
					break;
				case FSQL_TYPE_FLOAT:
					$typeMatch = '-?\d+\.\d+';
					$translateCode =  "((float) \$entry[$i])";
					break;
				default:
					$typeMatch = "'.*?(?<!\\\\\\\\)'";
					$translateCode = "substr(\$entry[$i], 1, -1)";
					break;
			}
			if($column['null']) {
				$readString .= "($typeMatch|NULL);";
				$translate[] = "((\$entry[$i] !== 'NULL') ? $translateCode : null)";
			} else {
				$readString .= "($typeMatch);";
				$translate[] = $translateCode;
			}
			$i++;
		}
					
		$fullTranslateCode = implode(',', $translate);
		$readCode = <<<EOC
\$line = rtrim(file_read_line(\$dataHandle));
if(preg_match("/{$readString}/", \$line, \$entry))
{
	array_shift(\$entry);
	\$row = (int) array_shift(\$entry);
	\$entries[\$row] = array($fullTranslateCode); 
}
EOC;
		
		$this->readFunction = create_function('$dataHandle,&$entries', $readCode);
	}
	
	function getColumns()
	{
		$this->columnsLockFile->acquireRead();
		$lock = $this->columnsLockFile->getHandle();
		
		$modified = fread($lock, 20);
		if($this->columnsLoad === null || $this->columnsLoad < $modified)
		{
			$this->columnsLoad = $modified;
			
			$this->columnsFile->acquireRead();
			$columnsHandle = $this->columnsFile->getHandle();

			$line = file_read_line($columnsHandle);		
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return NULL;
			}
			
			$num_columns = (int) $matches[1];
			for($i = 0; $i < $num_columns; $i++) {
				$line =	file_read_line($columnsHandle);
				if(preg_match("/(\S+): ([a-z][a-z]?);(.*);(0|1);(-?\d+(?:\.\d+)?|'(.*)'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
					$type = $matches[2];
					$default = $matches[5];
					if($default === 'NULL')
						$default = NULL;
					else if($default{0} === '\'')
						$default = $matches[6];
					else if($type === FSQL_TYPE_INTEGER)
						$default = (int) $default;
					else if($type === FSQL_TYPE_FLOAT)
						$default = (float) $default;

					$this->columns[$matches[1]] = array(
						'type' => $type, 'auto' => (bool) $matches[4], 'default' => $default, 'key' => $matches[7], 'null' => (bool) $matches[8]
					);
					preg_match_all("/'.*?(?<!\\\\)'/", $matches[3], $this->columns[$matches[1]]['restraint']);
					$this->columns[$matches[1]]['restraint'] = $this->columns[$matches[1]]['restraint'][0];
				} else {
					return null;
				}
			}

			$this->columnsFile->releaseRead();
			
			$this->_buildReadWriteFuncs();
		}
		
		$this->columnsLockFile->releaseRead();
		
		return $this->columns;
	}
	
	function setColumns($columns)
	{
		$this->columns = $columns;
		$this->_buildReadWriteFuncs();
		
		$toprint = count($columns)."\r\n";
		if(!empty($columns))
		{
			foreach($columns as $name => $column)
			{
				$default = $column['default'];
				if($default === NULL)
					$default = 'NULL';
				else if(is_string($default))
					$default = "'".$default."'";
				
				$restraint = is_array($column['restraint']) ? implode(',', $column['restraint']) : '';
				
				$toprint .= sprintf("%s: %s;%s;%d;%s;%s;%d;\r\n", $name, $column['type'], $restraint, $column['auto'], $default, $column['key'], $column['null']);
			}
		}
		
		$this->columnsLockFile->acquireWrite();
		$lock = $this->columnsLockFile->getHandle();
		$modified = fread($lock, 20);

		list($msec, $sec) = explode(' ', microtime());
		$this->columnsLoad = $sec.$msec;
		fseek($lock, 0, SEEK_SET);
		fwrite($lock, $this->columnsLoad);

		$this->columnsFile->acquireWrite();
		$columnsHandle = $this->columnsFile->getHandle();
		ftruncate($columnsHandle, 0);
		fwrite($columnsHandle, $toprint);
	
		$this->columnsFile->releaseWrite();
		$this->columnsLockFile->releaseWrite();
	}
	
	function isReadLocked()
	{
		return $this->lock === 'r';
	}

	function readLock()
	{
		$success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead();
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
		$success = $this->columnsLockFile->acquireWrite() && $this->columnsFile->acquireWrite();
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
		}
		else if($this->lock === 'w')
		{
			$this->columnsLockFile->releaseWrite();
			$this->columnsFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

/**
 * Class for the standard fSQL tables that are saved to the filesystem.
 */
class fSQLStandardTable extends fSQLTable
{
	var $rcursor = null;
	var $wcursor = null;
	var $entries = null;
	var $data_load = null;
	var $dataLockFile = null;
	var $dataFile = null;
	var $lock = null;
	
	function fSQLStandardTable($name, &$schema)
	{
		$this->name = $name;
		$this->schema =& $schema;
		$path_to_schema = $schema->getPath();
		$columns_path = $path_to_schema.$name.'.columns';
		$data_path = $path_to_schema.$name.'.data';
		$this->definition = new fSQLStandardTableDef($columns_path);
		$this->dataLockFile = new fSQLFile($data_path.'.lock.cgi');
		$this->dataFile = new fSQLFile($data_path.'.cgi');
	}
	
	function create($columnDefs)
	{
		$this->definition->setColumns($columnDefs);
		
		list($msec, $sec) = explode(' ', microtime());
		$this->data_load = $sec.$msec;

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

	function temporary() {
		return false;
	}
	
	function getColumnNames() {
		return array_keys($this->definition->getColumns());
	}
	
	function getColumns() {
		return $this->definition->getColumns();
	}
	
	function &getCursor()
	{
		$this->_loadEntries();

		if($this->rcursor === NULL)
			$this->rcursor = new fSQLCursor($this->entries);
		
		return $this->rcursor;
	}
	
	function &getWriteCursor()
	{
		$this->_loadEntries();

		if($this->wcursor === null)
			$this->wcursor = new fSQLWriteCursor($this->entries);
		
		return $this->wcursor;
	}
	
	function getEntries()
	{
		$this->_loadEntries();
		return $this->entries;
	}

	function _loadEntries()
	{
		$this->dataLockFile->acquireRead();
		$lock = $this->dataLockFile->getHandle();
		
		$modified = fread($lock, 20);
		if($this->data_load === null || $this->data_load < $modified)
		{
			$this->data_load = $modified;

			$this->dataFile->acquireRead();
			$dataHandle = $this->dataFile->getHandle();

			$line = file_read_line($dataHandle);
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->dataFile->releaseRead();
				$this->dataLockFile->releaseRead();
				return NULL;
			}
	
			$num_entries = (int) rtrim($matches[1]);
			$entries = array();
			
			if($num_entries != 0)
			{
				$readFunction = $this->definition->getReadFunction();
				for($i = 0;  $i < $num_entries; $i++) {
					$readFunction($dataHandle, $entries);
				}
			}
			
			$this->entries = $entries;

			$this->dataFile->releaseRead();
		}

		$this->dataLockFile->releaseRead();

		return true;
	}
	
	function commit()
	{
		$writeCursor =& $this->getWriteCursor();
		if($writeCursor->isUncommitted() === false)
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
						$toprint .= 'NULL;';
					else if(is_string($value))
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

		$this->wcursor = null;
	}
	
	function rollback()
	{
		$this->data_load = 0;
		$this->uncommited = false;
	}

	function drop()
	{
		if($this->lock === null)
		{
			$this->definition->drop();
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
		$success = $this->definition->unlock() && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
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
		$success = $this->definition->writeLock() && $this->dataLockFile->acquireWrite() && $this->dataFile->acquireWrite();
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
			$this->definition->unlock();
			$this->dataLockFile->releaseRead();
			$this->dataFile->releaseRead();
		}
		else if($this->lock === 'w')
		{
			$this->definition->unlock();
			$this->dataLockFile->releaseWrite();
			$this->dataFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

class fSQLStandardView extends fSQLView
{
	var $rcursor = null;
	var $queryLockFile = null;
	var $queryFile = null;
	var $queryLoad = null;
	var $lock = null;
	
	function fSQLStandardView($name, &$schema)
	{
		parent::fSQLView($name, $schema);
		$path_to_schema = $schema->getPath();
		$def_path = $path_to_schema.$name.'.view';
		$columns_path = $path_to_schema.$name.'.columns';
		$this->definition =& new fSQLStandardTableDef($columns_path);
		$this->queryLockFile =& new fSQLFile($def_path.'.lock.cgi');
		$this->queryFile =& new fSQLFile($def_path.'.cgi');
	}
	
	function define($query, $columns)
	{
		list($msec, $sec) = explode(' ', microtime());
		$this->queryLoad = $sec.$msec;

		// create the view lock
		$this->queryLockFile->acquireWrite();
		$queryLock = $this->queryLockFile->getHandle();
		ftruncate($queryLock, 0);
		fwrite($queryLock, $this->queryLoad);
		
		// create the view file
		$this->queryFile->acquireWrite();
		$definition = $this->queryFile->getHandle();
		ftruncate($definition, 0);
		fwrite($definition, $query);
		
		$this->queryFile->releaseWrite();	
		$this->queryLockFile->releaseWrite();
		
		$this->setQuery($query);
		$this->definition->setColumns($columns);
		$this->execute();
	}
	
	function drop()
	{
		if($this->lock === null)
		{
			$this->definition->drop();
			unlink($this->queryFile->getPath());
			unlink($this->queryLockFile->getPath());
			$this->close();
			return true;
		}
		else
			return false;
	}
	
	function temporary()
	{
		return false;
	}
	
	function getColumnNames() {
		return array_keys($this->definition->getColumns());
	}
	
	function getColumns() {
		return $this->definition->getColumns();
	}
	
	function setColumns($columns) {
		$this->definition->setColumns($columns);
	}
	
	function getEntries() {
		return $this->entries;
	}
	
	function &getCursor()
	{
		if($this->rcursor === NULL)
			$this->rcursor =& new fSQLCursor($this->entries);

		return $this->rcursor;
	}
	
	function _loadView()
	{
		$this->queryLockFile->acquireRead();
		$lock = $this->queryLockFile->getHandle();
		
		$modified = fread($lock, 20);
		if($this->queryLoad === null || $this->queryLoad < $modified)
		{
			$this->queryLoad = $modified;

			$this->queryFile->acquireRead();
			$dataHandle = $this->queryFile->getHandle();

			$this->query = file_read_line($dataHandle);

			$this->queryFile->releaseRead();
		}

		$this->queryLockFile->releaseRead();

		return true;
	}
	
	function execute()
	{
		$this->definition->getColumns();
		$this->_loadView();
		return parent::execute();
	}
	
	function isReadLocked()
	{
		return $this->lock === 'r';
	}

	function readLock()
	{
		$success = $this->definition->readLock() && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
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
		$success = $this->definition->writeLock() && $this->dataLockFile->acquireWrite() && $this->dataFile->acquireWrite();
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
			$this->definition->unlock();
			$this->dataLockFile->releaseRead();
			$this->dataFile->releaseRead();
		}
		else if($this->lock === 'w')
		{
			$this->definition->unlock();
			$this->dataLockFile->releaseWrite();
			$this->dataFile->releaseWrite();
		}
		$this->lock = null;
		return true;
	}
}

?>