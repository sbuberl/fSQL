<?php

/*
 * File only included if running PHP 4.
 * Implements possibly missing functions
 * used by fSQL.
 * 
 * Some implementations taken from PHP_Compat.
 * 
 * Implemented functions:
 *		- array_combine - 5.0.0
 *		- array_fill - 4.2.0
 *		- is_a - 4.2.0
 *		- array_key_exists - 4.2.0 (renamed from key_exists - 4.0.7)
 *		- vsprintf - 4.1.0
 */

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

if (!function_exists('array_fill'))
{
   function array_fill($start_index, $num, $value)
   {
	    if ($num <= 0) {
	        user_error('array_fill(): Number of elements must be positive', E_USER_WARNING);
	        return false;
	    }
	
	    $temp = array();
	
	    if ($start_index < 0) {
	        $temp[$start_index] = $value;
	        $start_index = 0;
	        $end_index = $num - 1;
	    } else {
	        $end_index = $start_index + $num;
	    }
	
	    for ($i = (int) $start_index; $i < $end_index; $i++) {
	        $temp[$i] = $value;
	    }
	
	    return $temp;
   }
}

if(!function_exists('is_a'))
{
	function is_a($anObject, $aClass)
	{
	   return !strcasecmp(get_class($anObject), $aClass) || is_subclass_of($anObject, $aClass);
	}
}

if(!function_exists('array_key_exists'))
{
	function array_key_exists($key, $search) {
      return in_array($key, array_keys($search));
   }
}

if(!function_exists('vsprintf'))
{
    function vsprintf($format, $args)
    {
        if (count($args) < 2) {
            trigger_error('vsprintf() Too few arguments', E_USER_WARNING);
            return;
        }

        array_unshift($args, $format);
        return call_user_func_array('sprintf', $args);
    }
}

// Portable recursive mkdir wrapper (recursive flag added in PHP 5)
function mkdir_recursive($pathname, $mode)
{
	is_dir(dirname($pathname)) || mkdir_recursive(dirname($pathname), $mode);
	return is_dir($pathname) || mkdir($pathname, $mode);
}

// Portable is_a() wrapper (uses is_a())
function fsql_is_a($object, $classname)
{
	return is_a($object, $classname);
}

// Portable wrapper for loading class_exists that does not ever
// autoload (like PHP 5 does by default)
function fsql_class_exists($className)
{
	return class_exists($className);
}

?>