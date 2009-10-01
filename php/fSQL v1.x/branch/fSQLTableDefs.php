<?php

class fSQLTableDef
{	
	function close() { return false; }
	function drop() { return false; }

	function getColumnNames() {
		$columns = $this->getColumns();
		return $columns !== false ? array_keys($columns) : false;
	}
	function getColumns() { return false; }
	function setColumns($columns) { }
	
	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
}

class fSQLMemoryTableDef extends fSQLTableDef
{
	var $columns = null;
	
	function fSQLMemoryTableDef()
	{
		
	}
	
	function close()
	{
		$this->columns = null;
	}
	
	function drop()
	{
		$this->close();
		return true;
	}

	function getColumns()
	{
		return $this->columns;
	}
	
	function setColumns($columns)
	{
		$this->columns = $columns;
	}
	
	function isReadLocked() { return false; }
	function readLock() { return false; }
	function writeLock() { return false; }
	function unlock() { return false; }
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
\$line = rtrim(fgets(\$dataHandle, 4096));
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

			$line = fgets($columnsHandle);		
			if(!preg_match('/^(\d+)/', $line, $matches))
			{
				$this->columnsFile->releaseRead();
				$this->columnsLockFile->releaseRead();
				return NULL;
			}
			
			$num_columns = (int) $matches[1];
			for($i = 0; $i < $num_columns; $i++) {
				$line =	fgets($columnsHandle);
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

?>