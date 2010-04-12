<?php

// Define PHP_VERSION_ID if not defined (introduced in PHP 5.2.7)
if (!defined('PHP_VERSION_ID')) {
    $version = explode('.', PHP_VERSION);
    define('PHP_VERSION_ID', ($version[0] * 10000 + $version[1] * 100 + $version[2]));
}

// Include PHP4 compatibility file if running PHP 4
if(PHP_VERSION_ID >= 50000)
	require FSQL_INCLUDE_PATH.'/fSQLPHP5Compat.php';
else
	require FSQL_INCLUDE_PATH.'/fSQLPHP4Compat.php';

// Define array_intersect_key if missing (introduced in PHP 5.1.0)
if(!function_exists('array_intersect_key'))
{
	function array_intersect_key()
	{
	    $args = func_get_args();
	    $array_count = count($args);
	    if ($array_count < 2) {
	        user_error('Wrong parameter count for array_intersect_key()', E_USER_WARNING);
	        return;
	    }
	
	    // Check arrays
	    for ($i = $array_count; $i--;) {
	        if (!is_array($args[$i])) {
	            user_error('array_intersect_key() Argument #' .
	                ($i + 1) . ' is not an array', E_USER_WARNING);
	            return;
	        }
	    }
	
	    // Intersect keys
	    $arg_keys = array_map('array_keys', $args);
	    $result_keys = call_user_func_array('array_intersect', $arg_keys);
	    
	    // Build return array
	    $result = array();
	    foreach($result_keys as $key) {
	        $result[$key] = $args[0][$key];
	    }
	    return $result;
	}

}

/* Wrapper around fgets() to make length optional (introduced in PHP 4.2) */
if(PHP_VERSION_ID >= 40200)
{
	function file_read_line($file)
	{
		return fgets($file);
	}
}
else
{
	function file_read_line($file)
	{
		$line = '';
		$ending = null;
		do
		{
			$read = fgets($file, 1024);
			if($read)
			{
				$line .= $read;
				if(strlen($read) === 1024)
				{
					$ending = substr($read, -1);
					if($ending !== "\r" && $ending !== "\n")
						continue;
				}
			}
			
			break;
		} while(true);
		
		return $line;
	}
}

// fSQL LIKE uses eregi.  ereg functions are deprecated in PHP 5.3 and
// removed in PHP 6 so create a wrapper function that uses
// it where we can and uses preg_match where we can't.
if(PHP_VERSION_ID < 50300 && function_exists('eregi'))
{
	function fsql_eregi($pattern, $string)
	{
		return eregi($pattern, $string);
	}
}
else
{
	function fsql_eregi($pattern, $string)
	{
		// Is there anything else to capture?
		// PCRE supports POSIX character classes so all
		// POSIX syntax should be the same on PCRE.  We'll see.
	
		// add delimiters, escape them, add i flag,
		// and then call preg_match
		$string = '/'.addcslashes($string, '/').'/i';
		return preg_match($pattern, $string);
	}
}

?>