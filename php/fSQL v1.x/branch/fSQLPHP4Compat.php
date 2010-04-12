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

?>