<?php

fsql_load_class('fSQLStandardFunctions', FSQL_FRONTENDS_PATH.'/standard/');

class fSQLMysqlFunctions extends fSQLStandardFunctions
{
	function getFunctionInfo($function_name)
	{
		static $functions = array(
			'acos' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'ascii' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'asin'  => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'atan' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'atan2' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'bin' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'bit_length' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'char' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'concat_ws' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'conv' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'cos' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'cot' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'crc32' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'dayofweek' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'dayofyear' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'degrees' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'elt' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'export_set' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'field' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'find_in_set' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'format' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'from_unixtime' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_DATETIME, true),
			'hex' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'insert' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'instr' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'last_insert_id' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'left' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'locate' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'log' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'log2' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'log10' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'lpad' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'ltrim' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'make_set',
			'md5' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'oct' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'pi' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, false),
			'quote' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'radians' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'rand' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'repeat' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'replace' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'reverse' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'right' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'round' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'row_count' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'rpad' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'rtrim' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'sha1' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'sign' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'sin'=> array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'soundex' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'space' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'strcmp' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'substring_index' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'tan' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'truncate' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'unhex' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'unix_timestamp'  => array(FSQL_FUNC_NORMAL, FSQL_TYPE_TIMESTAMP, true),
			'weekday' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true)
		);
		
		static $renamed_funcs = array(
			'curdate' => 'current_date',
			'curtime' => 'localtime',
			'database' => 'current_catalog',
			'day' => 'dayofmonth',
			'lcase' => 'lower',
			'length' => 'char_length',
			'mid' => 'substring',
			'now' => 'localtimestamp',
			'ord' => 'ascii',
			'pow' => 'power',
			'schema' => 'current_catalog',
			'sha' => 'sha1',
			'substr' => 'substring',
			'ucase' => 'upper'
		);
		
		if(isset($renamed_funcs[$function_name]))
			$function_name = $renamed_funcs[$function_name];
		
		if(isset($functions[$function_name]))
			return $functions[$function_name];
		else
			parent::getFunctionInfo($function_name);
	}
	
	//////Misc Functions
	function last_insert_id($env)
	{
		return $this->enviroment->insert_id;
	}
	function md5($string)
	{
		return ($string !== null) ? md5($string) : null;
	}
	function row_count($env)
	{
		return $this->environment->affected;
	}
	function sha1($string)
	{
		return ($string !== null) ? sha1($string) : null;
	}

	/////Math Functions
	function acos($arg)
	{
		if(fSQLTypes::forceFloat($arg) && $arg >= -1.0 && $arg <= 1.0)
			return acos($arg);
		else
			return null;
	}
	function asin($arg)
	{
		if(fSQLTypes::forceFloat($arg) && $arg >= -1.0 && $arg <= 1.0)
			return asin($arg);
		else
			return null;
	}
	function atan($arg)
	{
		if(fSQLTypes::forceFloat($arg))
		{
			if(func_num_args() === 2)
			{
				$arg2 = func_get_arg(1);
				if(fSQLTypes::forceFloat($arg2))
					return atan2($arg, $arg2);
			}
			else
				return atan($arg);
		}
		return null;
	}
	function atan2($y, $x)
	{
		if(fSQLTypes::forceFloat($y) && fSQLTypes::forceFloat($x))
			return atan2($y, $x);
		else
			return null;
	}
	function conv($number, $frombase, $tobase)
	{
		if($number !== null && fSQLTypes::forceInteger($frombase) && fSQLTypes::forceInteger($tobase))
			return base_convert($number, $frombase, $tobase);
		else
			return null;
	}
	function cos($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return cos($arg);
		else
			return null;
	}
	function cot($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return tan(M_PI_2 - $arg);
		else
			return null;
	}
	function crc32($arg)
	{
		if($arg !== null)
			return crc32($arg);
		else
			return null;
	}
	function degrees($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return rad2deg($arg);
		else
			return null;
	}
	function format($number, $places)
	{
		if(fSQLTypes::forceFloat($number) && fSQLTypes::forceInteger($places))
			return number_format($number, $places);
		else
			return null;
	}
	function hex($arg)
	{
		if(fSQLTypes::forceInteger($arg))
			return dechex($arg);
		else
			return null;
	}
	function log()
	{
		$num_args = func_num_args();
		$value = func_get_arg($num_args - 1);
		if(fSQLTypes::forceFloat($value) && $value >= 0.0)
		{
			if($num_args === 2)
			{
				$base = func_get_arg(0);
				if(fSQLTypes::forceFloat($base) && $base > 0.0 && $base !== 1.0)
					return log($value) / log($base);
			}
			else
				return log($value);
		}
		
		return null;
	}
	function log2($arg)
	{
		if(fSQLTypes::forceFloat($arg) && $arg >= 0.0)
			return log($arg) / M_LN2;
		else
			return null;
	}
	function log10($arg)
	{
		if(fSQLTypes::forceFloat($arg) && $arg >= 0.0)
			return log10($arg);
		else
			return null;
	}
	function oct($arg)
	{
		if(fSQLTypes::forceInteger($arg))
			return decoct($arg);
		else
			return null;
	}
	function pi()
	{
		return M_PI;
	}
	function radians($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return deg2rad($arg);
		else
			return null;
	}
	function rand()
	{
		$num_args = func_num_args();
		if($num_args === 0) {
			return lcg_value();
		}
		
		$seed = func_get_arg(0);
		if(fSQLTypes::forceInteger($seed, true))
		{
			srand((int)$seed);
			return lcg_value();
		}
		else
			return null;
	}
	function round($number)
	{
		if(fSQLTypes::forceInteger($number))
		{
			$num_args = func_num_args();
			if($num_args === 2) {
				$places = func_get_arg(1);
				if(fSQLTypes::forceFloat($places))
					return round($number, round($places));
			} else {
				return round($number);
			}
		}
		
		return null;
	}
	function sign($number) {
		if(fSQLTypes::forceNumber($number))
		{
			if($number > 0)			return 1;
			else if($number == 0)	return 0;
			else					return -1;
		}
		else
			return null;
	}
	function sin($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return sin($arg);
		else
			return null;
	}
	function tan($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return tan($arg);
		else
			return null;
	}
	function truncate($number, $places) {
		if(fSQLTypes::forceFloat($number) && fSQLTypes::forceNumber($places))
		{
			list($integer, $decimals) = explode('.', $number);
			if($places == 0) { $value = $integer; }
			else if($places > 0) { $value =  $integer.'.'.substr($decimals,0,$places); }
			else {   $value = substr($integer,0,$places) * pow(10, abs($places));  }
			return (float) $value;
		}
		else
			return null;
	}
	
	/////String Functions
	function ascii($string) {
		return ($string !== null) ? ord($string) : null;
	} 
	function bin($string) {
		return fSQLTypes::forceInteger($string) ? decoct($string) : null;
	}
	function bit_length($string) {
		return ($string !== null) ? strlen($string) << 3 : null;
	}
	function char() {
		$return = array();
		$args = func_get_args();
		$args = array_filter($args, 'is_null');
		foreach($args as $arg)
			$return[] = chr($arg);
		return implode('', $return);
	}
	function concat_ws($sep) {
		$args = func_get_args();
		if($sep !== null) {
			array_shift($args);
			return implode($sep, $args);
		}
		else
			return null;
	}
	function elt($index) {
		$num_eles = func_num_args(0);
		if(index >= 1 && $index <= $num_eles) {	return func_get_arg($index);  }
		else { return NULL; }
	}
	function export_set($bits, $on, $off, $seperator = ',', $number_of_bits = 32) {
		if(fSQLTypes::forceInteger($bits) && $on !== null && $off != null && $seperator !== null && fSQLTypes::forceInteger($number_of_bits))
		{
			$string = strrev(decbin($bits));
			$string = str_pad($string, $number_of_bits, "0", STR_PAD_LEFT);
			$string = strtr($string, array('1' => $on, '0' => $off));
			return implode($seperator, explode('', $string));
		}
		else
			return null;
	}
	function field() {
		$numargs = func_num_args();
		$args = func_get_args();
		$find = array_shift($args);
		$index = array_search($find, $args);
		if($index !== false)
			return $index + 1;
		else
			return 0;
	}
	function find_in_set($find, $set) {
		$index = array_search($find, explode(',', $set));
		if($index !== false)
			return $index + 1;
		else
			return 0;
	}
	function insert($str, $pos, $len, $newstr) {
		if($str !== null && fSQLTypes::forceInteger($pos) && fSQLTypes::forceInteger($len) && $newstr !== null)
			return substr_replace($str, $newstr, $pos, $len);
		else
			return null;
	}
	function instr($string, $find) {
		$pos = strpos($string, $find);
		if($pos !== false) { return $pos + 1; } else { return 0; }
	}
	function left($string, $length) {
		if($string !== null && fSQLTypes::forceInteger($length))
			return substr($string, 0, $length);
		else
			return null;
	}
	function locate($string, $find, $start = 0) {
		if($string !== null && $find !== null && fSQLTypes::forceInteger($start))
		{
			$pos = strpos($string, $find, $start);
			if($pos !== false) { return $pos + 1; } else { return 0; }
		}
		else
			return null;
	}
	function lpad($string, $length, $pad) { 
		if($string !== null && fSQLTypes::forceInteger($length) && $pad !== null)
			return str_pad($string, $length, $pad, STR_PAD_LEFT);
		else
			return null;
	}
	function ltrim($string) {
		return ($string !== null) ? ltrim($string) : null;
	}
	function quote($string) {
		return ($string !== null) ? addslashes($string) : null;
	}
	function repeat($string, $times) {
		if($string !== null && fSQLTypes::forceInteger($times))
			return str_repeat($string);
		else
			return null;
	}
	function reverse($string) {
		return ($string !== null) ? strrev($string) : null;
	}
	function right($string, $length) {
		if($string !== null && fSQLTypes::forceInteger($length))
			return substr($string, -$length);
		else
			return null;
	}
	function rpad($string, $length, $pad) { 
		if($string !== null && fSQLTypes::forceInteger($length) && $pad !== null)
			return str_pad($string, $length, $pad, STR_PAD_RIGHT);
		else
			return null;
	}
	function rtrim($string) {
		return ($string !== null) ? rtrim($string) : null;
	}
	function soundex($string) {
		return ($string !== null) ? soundex($string) : null;
	}
	function space($number)	{
		return fSQLTypes::forceInteger($number) ? str_repeat(' ', $number) : null;
	}
	function strcmp($left, $right)	{
		return ($left !== null && $right !== null) ? strcmp($left, $right) : null;
	}
	function substring_index($string, $delim, $count) {
		if($string !== null && $delim !== null && fSQLTypes::forceInteger($count))
		{
			$parts = explode($delim, $string);
			if($count < 0)
				$array = array_slice($parts, $count);
			else 
				$array = array_slice($parts, 0, $count);
			return implode($delim, $array);
		}
		else
			return null;
	}
	function unhex($string) {
		return ($string !== null) ? hexdec($string) : null;
	}
	 
	////Date/Time functions
	function dayofmonth($date) {
		return ($date !== null) ? (int) strftime('%d', $date) : null;
	}
	function dayofweek($date) {
		return ($date !== null) ? (int) strftime('%u', $date) : null;
	}
	function dayofyear($date) {
		return ($date !== null) ? (int) strftime('%j', $date) : null;
	}
	function from_unixtime($timestamp, $format = FSQL_FORMAT_DATETIME)
	{
		if(!is_int($timestamp)) { $timestamp = FSQLFunctions::unix_timestamp($timestamp); }
		return strftime($format, $timestamp);
	}
	function unix_timestamp() {
		$num_args = func_num_args();
		if($num_args === 0) {
			return time();
		} else {
			$datetime = func_get_arg(0);
			$unix_time = 0;
			if(preg_match('/(\d{2}(?:\d{2})?)(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/', $datetime, $matches)) {
				$unix_time = mktime($matches[4], $matches[5], $matches[6], $matches[2], $matches[3], $matches[1]);
			} else if (preg_match('/\d{2}(?:\d{2})?\-\d{2}\-\d{2}(?: \d{2}:\d{2}:\d{2})?/', $datetime)) {
				$unix_time = strtotime($datetime);
			} else if (preg_match('/(\d{2}(?:\d{2})?)(\d{2})(\d{2})/', $datetime, $matches)) {
				$unix_time = mktime(0, 0, 0, $matches[2], $matches[3], $matches[1]);
			}
			return $unix_time; 
		}
	}
	function weekday($date) {
		return ($date !== null) ? (int) strftime('%w', $date) : null;
	}
}

?>