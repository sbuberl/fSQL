<?php

// Portable wrapper for loading classes without autoload
function fsql_load_class($className, $path)
{
	if(!fsql_class_exists($className))
	{
		$fullPath = "$path/$className.php";
		if(file_exists($fullPath))
		{
			require $fullPath;
			return true;
		}
		else
			return false;		
	}
	else
		return true;
}

function create_directory($original_path, $type, &$environment)
{
	$paths = pathinfo($original_path);
	
	$dirname = realpath($paths['dirname']);
	if(!$dirname || !is_dir($dirname) || !is_readable($dirname)) {
		if(mkdir_recursive($original_path, 0777) === true)
			$realpath = $original_path;
		else
			return $environment->_set_error(ucfirst($type)." parent path '{$paths['dirname']}' does not exist.  Please correct the path or create the directory.");
	}
	
	$path = $dirname.'/'.$paths['basename'];
	$realpath = realpath($path);
	if($realpath === false || !file_exists($realpath)) {
		if(@mkdir_recursive($path, 0777) === true)
			$realpath = $path;
		else
			return $environment->_set_error("Unable to create directory '$path'.  Please make the directory manually or check the permissions of the parent directory.");
	} else if(!is_readable($realpath) || !is_writeable($realpath)) {
		@chmod($realpath, 0777);
	}

	if(substr($realpath, -1) != '/')
		$realpath .= '/';

	if(is_dir($realpath) && is_readable($realpath) && is_writeable($realpath)) {
		return $realpath;
	} else {
		return $environment->_set_error("Path to directory for $type is not valid.  Please correct the path or create the directory and check that is readable and writable.");
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

	function close()
	{
		// should be unlocked before reaches here, but just in case,
		// release all locks and close file
		if(isset($this->handle))
		{
		//	flock($this->handle, LOCK_UN);
			fclose($this->handle);
		}
		unset($this->filepath, $this->handle, $this->lock);
	}
	
	function drop()
	{
		// only allow drops if not locked
		if($this->handle === null)
		{
			unlink($this->filepath);
			$this->close();
			return true;
		}
		else
			return false;
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

class fSQLOrderByClause
{
	var $sortFunction;
	
	function fSQLOrderByClause($tosortData)
	{
		$code = "";
		foreach($tosortData as $tosort) {
			$key = $tosort['key'];
			if($tosort['ascend'])
			{
				$ltVal = -1;
				$gtVal = 1;
			}
			else
			{
				$ltVal = 1;
				$gtVal = -1;
			}
			$code .= <<<EOC
\$a_value = \$a[$key];
\$b_value = \$b[$key];
if(\$a_value === null)		return $ltVal;
elseif(\$b_value === null)	return $gtVal;
elseif(\$a_value < \$b_value)	return $ltVal;
elseif(\$a_value > \$b_value)	return $gtVal;
EOC;
		}
		$code .= 'return 0;';
		$this->sortFunction = create_function('$a, $b', $code);
	}	

	function sort(&$data)
	{
		usort($data, $this->sortFunction);
	}
}

class fSQLTypes
{
	function to_float($arg) {
		if($arg === null)
			return null;
		else if(!is_numeric($arg))
			return false;
		return (float) $arg;
	}
	
	function to_int($arg) {
		if($arg === null)
			return null;
		else if(!is_numeric($arg))
			return false;	
		return !is_int($arg) ? (int) $arg : $arg;
	}
	
	function to_number($arg) {
		if($arg === null)
			return null;
		else if(!is_numeric($arg))
			return false;
		else if(is_string($arg))
			$arg = strpos($arg, '.') === false ? (int) $arg : (float) $arg;
		return true;
	}
	
	function to_string($arg) {
		if($arg === null)
			return null;
		return !is_string($arg) ? (string) $arg : $arg;
	}
	
	function forceFloat(&$arg, $nullable = false) {
		if($arg === null)
			return $nullable;
		else if(!is_numeric($arg))
			return false;
		
		$arg = (float) $arg;
		return true;
	}
	
	function forceInteger(&$arg, $nullable = false) {
		if($arg === null)
			return $nullable;
		else if(!is_numeric($arg))
			return false;
		else if(!is_int($arg))
			$arg = (int) $arg;
		return true;
	}
	
	function forceNumber(&$arg, $nullable = false) {
		if($arg === null)
			return $nullable;
		else if(!is_numeric($arg))
			return false;
		else if(is_string($arg))
			$arg = strpos($arg, '.') === false ? (int) $arg : (float) $arg;
		return true;
	}
	
	function forceString(&$arg, $nullable = false)
	{
		if($arg === null)
			return $nullable;
		else if(is_float($arg) || is_integer($arg))
			$arg = (string) $arg;
		return true;
	}
	
	function isTrue($expr)
	{
		return !in_array($expr, array(0, 0.0, '', null), true);
	}
	
	function isFalse($expr)
	{
		return in_array($expr, array(0, 0.0, ''), true);
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
}

?>