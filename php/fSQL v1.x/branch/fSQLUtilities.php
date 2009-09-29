<?php

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

function same_obj(&$a, &$b) {

    if(gettype($a) !== gettype($b)) return false;

    $same = false;
    if(is_array($a)){

        //Look for an unused index in $a
        $key = uniqid("is_ref_", true);
        while(isset($a[$key]))$key = uniqid("is_ref_", true);

        //The two variables differ in content ... They can't be the same
        if(isset($b[$key])) return false;

        //The arrays point to the same data if changes are reflected in $b
        $data = uniqid("is_ref_data_", true);
        $a[$key] =& $data;
        //There seems to be a modification ...
        $same = ((isset($b[$key])) and ($b[$key] === $data));

        //Undo our changes ...
        unset($a[$key]);

    }elseif(is_object($a)){

        //The same objects are required to have equal class names ;-)
        if(get_class($a) !== get_class($b)) return false;

        //Look for an unused property in $a
        $key = uniqid("is_ref_", true);
        while(isset($a->$key))$key = uniqid("is_ref_", true);

        //The two variables differ in content ... They can't be the same
        if(isset($b->$key)) return false;

        //The arrays point to the same data if changes are reflected in $b
        $data = uniqid("is_ref_data_", true);
        $a->$key =& $data;
        //There seems to be a modification ...
        $same = ((isset($b->$key)) and ($b->$key === $data));

        //Undo our changes ...
        unset($a->$key);

    }elseif(is_resource($a)){

        if(get_resource_type($a) !== get_resource_type($b))return false;
        $same = ((string) $var1) === ((string) $var2);

    }else{

        if($a !== $b) return false;

        //To check for a reference of a variable with simple type
        //simply store its old value and check against modifications of the second variable ;-)

        $data = uniqid("is_ref_", true);
        while($data === $a) $data = uniqid("is_ref_", true);

        $save = $a;             //WE NEED A COPY HERE!!!
        $a    = $data;          //Set a to the value of $data (copy)
        $same = ($a === $b);    //Check if $var2 was modified too ...
        $a    = $save;          //Undo our changes ...

    }
    return $same;
}

if(!function_exists('is_a'))
{
	function is_a($anObject, $aClass)
	{
	   return !strcasecmp(get_class($anObject), $aClass) || is_subclass_of($anObject, $aClass);
	}
}

function create_directory($original_path, $type, &$environment)
{
	$paths = pathinfo($original_path);
	
	$dirname = realpath($paths['dirname']);
	if(!$dirname || !is_dir($dirname) || !is_readable($dirname)) {
		return $this->environment->_set_error(ucfirst($type)." parent path '$path' does not exist.  Please correct the path or create the directory.");
	}
	
	$path = $dirname.'/'.$paths['basename'];
	$realpath = realpath($path);
	if($realpath === false) {
		if(@mkdir($path, 0777) === true)
			$realpath = $path;
		else
			return $environment->_set_error("Unable to create directory '$path'.  Please make the directory manually or check the permissions of the parent directory.");
	} else if(!is_readable($path) || !is_writeable($path)) {
		chmod($path, 0777);
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

class fSQLTypes
{
	function forceFloat(&$arg, $nullable = false) {
		if($arg == null)
			return $nullable;
		else if(!is_numeric($arg))
			return false;
		
		$arg = (float) $arg;
		return true;
	}
	
	function forceInteger(&$arg, $nullable = false) {
		if($arg == null)
			return $nullable;
		else if(!is_numeric($arg))
			return false;
		else if(!is_float($arg))
			$arg = (float) $arg;
		return true;
	}
	
	function forceNumber(&$arg, $nullable = false) {
		if($arg == null)
			return $nullable;
		else if(!is_numeric($arg))
			return false;
		else if(is_string($arg))
			$arg = ctype_digit($arg) ? (int) $arg : (float) $arg;
		return true;
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