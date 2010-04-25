<?php

class fSQLStandardFunctions
{
	var $environment;
	
	function fSQLStandardFunctions(&$environment)
	{
		$this->environment =& $environment;
	}
	
	function getFunctionInfo($function_name)
	{
		static $functions = array(
			'abs' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_NUMERIC, true),
			'any' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_BOOLEAN, true),
			'avg' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_FLOAT, true),
			'cast' => array(FSQL_FUNC_CUSTOM_PARSE, FSQL_TYPE_STRING, true),
			'ceiling' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'count' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_INTEGER, false),
			'char_length' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'concat' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'current_catalog' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'current_date' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_DATE, false),
			'current_schema' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'current_time' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_TIME, false),
			'current_timestamp' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, false),
			'extract' => array(FSQL_FUNC_CUSTOM_PARSE, FSQL_TYPE_INTEGER, true),
			'every' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_BOOLEAN, true),
			'exp' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'floor' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true),
			'ln' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'localtime' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_TIME, false),
			'localtimestamp' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, false),
			'lower' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'max' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_FLOAT, true),
			'min' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_FLOAT, true),
			'mod' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_NUMERIC, true),
			'overlay' => array(FSQL_FUNC_CUSTOM_PARSE, FSQL_TYPE_STRING, true),
			'position' => array(FSQL_FUNC_CUSTOM_PARSE, FSQL_TYPE_INTEGER, true),
			'power' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'sqrt' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_FLOAT, true),
			'substring' => array(FSQL_FUNC_CUSTOM_PARSE, FSQL_TYPE_STRING, true),
			'sum' => array(FSQL_FUNC_AGGREGATE, FSQL_TYPE_FLOAT, true),
			'trim' => array(FSQL_FUNC_CUSTOM_PARSE, FSQL_TYPE_STRING, true),
			'upper' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, true),
			'version' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_STRING, false),
			'width_bucket' => array(FSQL_FUNC_NORMAL, FSQL_TYPE_INTEGER, true)
		);
		
		static $renamed_funcs = array(
			'ceil' => 'ceiling',
			'character_length' => 'char_length',
			'current_database' => 'current_catalog',
			'octet_length' => 'char_length',  // for now
			'some' => 'any'
		);
		
		if(isset($renamed_funcs[$function_name]))
			$function_name = $renamed_funcs[$function_name];
		
		return isset($functions[$function_name]) ? $functions[$function_name] : false;
	}
	
	//////Misc Functions
	function current_catalog()
	{
		$db = $this->environment->currentDB;
		return $db !== null ? $db->getName() : null;
	}
	
	function current_schema()
	{
		$schema = $this->environment->currentSchema;
		return $schema !== null ? $schema->getName() : null;
	}
	
	function version()
	{
		return FSQL_VERSION;
	}
	
	/////Math Functions
	function abs($arg)
	{
		if(fSQLTypes::forceNumber($arg))
			return abs($arg);
		else
			return null;
	}
	
	function ceiling($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return ceil($arg);
		else
			return null;
	}

	function exp($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return exp($arg);
		else
			return null;
	}
	
	function floor($arg)
	{
		if(fSQLTypes::forceFloat($arg))
			return floor($arg);
		else
			return null;
	}
	
	function ln($arg)
	{
		if(fSQLTypes::forceFloat($arg) && $arg >= 0.0)
			return log($arg);
		else
			return null;
	}

	function mod($one, $two) {
		if(fSQLTypes::forceNumber($one) && fSQLTypes::forceNumber($two) && $two != 0)
			return $one % $two;
		else
			return null;
	}
	
	function power($value, $exp) {
		if(fSQLTypes::forceFloat($value) && fSQLTypes::forceFloat($exp))
			return pow($value, $exp);
		else
			return null;
	}
	
	function sqrt($arg)
	{
		if(fSQLTypes::forceFloat($arg) && $arg >= 0.0)
			return sqrt($arg);
		else
			return null;
	}
	
	function width_bucket($arg, $min, $max, $buckets)
	{
		if(fSQLTypes::forceNumber($arg) && fSQLTypes::forceNumber($min) && fSQLTypes::forceNumber($max) && fSQLTypes::forceInteger($buckets))
		{
			if($arg < $min)
				return 0;
			else if($arg >= $max)
				return $buckets + 1;
				
			$bucketSize = ($max - $min + 1) / $buckets;
			return (int) floor(($arg - $min) / $bucketSize) + 1;
		}
		else
			return null;
	}
	 
	//// Aggregate Functions
	
	function any($data, $column, $flag) {
		if ($flag === "constant")
			return fSQLTypes::isTrue($data);
		foreach($data as $entry){
			if(fSQLTypes::isTrue($entry[$column]))
				return true;
		}
		return false;
	}
	
	function avg($data, $column, $flag) {
		$sum = fSQLFunctions::sum($data, $column, $flag);
		return $sum !== null ? $sum / count($data) : null;
	}
	
	function count($data, $column, $flag) {
		if($column == '*') { return count($data); }
		else if($flag === "constant") { return (int) ($column !== null); }
		else {
			$i = 0;
			foreach($data as $entry) {
				if($entry[$column] !== null) { $i++; }
			}
			return $i;
		}
	}
	
	function every($data, $column, $flag) {
		if ($flag === "constant")
			return fSQLTypes::isTrue($data);
		foreach($data as $entry){
			if(!fSQLTypes::isTrue($entry[$column]))
				return false;
		}
		return true;
	}
	
	function max($data, $column, $flag) {
		$max = null;
		if ($flag === "constant")
			$max = $column;
		else {
			foreach($data as $entry){
				if($entry[$column] > $max || $max === null) {
					$max = $entry[$column];
				} 
			}
		}
		return $max;
	}
	
	function min($data, $column, $flag) {
		$min = null;
		if ($flag === "constant")
			$min = $column;
		else {
			foreach($data as $entry){
				if($entry[$column] < $min || $min === null) {
					$min = $entry[$column];
				} 
			}
		}
		return $min;
	}
	
	function sum($data, $column, $flag) {
		$i = null;
		
		if ($flag === "constant" && $column !== null)
			$i = $column * sizeof($data);
		else {
			foreach($data as $entry)
			{
				$i += $entry[$column];
			}
		}

		return $i;
	}
	
	/////String Functions
	
	function char_length($string) {
		return ($string !== null) ? strlen($string) : null;
	}
	
	function concat() {
		$args = func_get_args();
		return (!in_array(null, $args, true)) ? implode('', $args) : null;
	}
	
	function lower($string) {
		return ($string !== null) ? strtolower($string) : null;
	}
	
	// not public
	function ltrim($string, $charlist = ' ') {
		return ($string !== null) ? ltrim($string, $charlist) : null;
	}
	
	function overlay($string, $other, $start = 1) {
		if($string !== null && $other !== null && fSQLTypes::forceInteger($start))
		{
			$start -= 1; // one-based not zero-based
			if(func_num_args() === 4) {
				$length = func_get_arg(3);
				if(fSQLTypes::forceInteger($length))
					return substr_replace($string, $other, $start, $length);
			}
			else
				return substr_replace($string, $other, $start);
		}
		
		return null;
	}
	
	function position($substring, $string) {
		if($string !== null && $substring !== null)
		{
			$pos = strpos($string, $substring);
			if($pos !== false) { return $pos + 1; } else { return 0; }
		}
		else
			return null;
	}
	
	// not public
	function rtrim($string, $charlist = ' ') {
		return ($string !== null) ? rtrim($string, $charlist) : null;
	}
	
	function substring($string, $pos) {
		if($string !== null && fSQLTypes::forceInteger($pos))
		{
			$pos -= 1; // one-based not zero-based
			if(func_num_args() === 3) {
				$length = func_get_arg(2);
				if(fSQLTypes::forceInteger($length))
					return substr($string, $pos, $length);
			}
			else
				return substr($string, $pos);
		}
		
		return null;
	}
	
	function trim($string, $charlist = ' ') {
		return ($string !== null) ? trim($string, $charlist) : null;
	}

	function upper($string) {
		return ($string !== null) ? strtoupper($string) : null;
	}
	 
	////Date/Time functions
	
	// Shared function for building time and timestamp strings
	// from unix timestamps inclduing microseconds if necessary
	function _build_time($format, $precision)
	{
		if(((int) $precision) !== 0)
		{
			list($msec, $seconds) = explode(' ', microtime());
			$msec = substr($msec, 2, $precision);
			return date($format, $seconds).'.'.$msec;
		}
		else
		{
			return date($format);
		}
	}
	
	function _build_time_with_tz($format, $precision)
	{
		$tz = substr_replace(date('O'), ':', 3, 0);
		return $this->_build_time($format, $precision).$tz;
	}
	
	function current_date()	{ 
		return date(FSQL_FORMAT_DATE);
	}
	
	function current_time($precision = 6) {
		return $this->_build_time_with_tz(FSQL_FORMAT_TIME, $precision);
	}
	
	function current_timestamp($precision = 6) {
		$tz = substr_replace(date('O'), ':', 3, 0);
		return $this->localtimestamp($precision).$tz;
	}
	
	function extract($field, $datetime)
	{
		if($datetime !== null && preg_match('/\A(((?:[1-9]\d)?\d{2})-(0\d|1[0-2])-([0-2]\d|3[0-1])\s*)?(\b([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)(?:\.(\d+))?(?:([\+\-]\d{2}):(\d{2}))?)?\Z/is', $datetime, $matches))
		{
			$hasDate = !empty($matches[1]);
			
			if($hasDate) // date or timestamp
			{
				$year = (int) $matches[2];
				$month = (int) $matches[3];
				$day = (int) $matches[4];
			}
			
			if(isset($matches[5])) // time or timestamp
			{
				$hour = (int) $matches[6];
				$minute = (int) $matches[7];
				$second = (int) $matches[8];
				if(isset($matches[9]))
					$microsecond = (int) $matches[9];
				else
					$microsecond = 0;
				if(isset($matches[10]))
				{
					$tz_hour = (int) $matches[10];
					$tz_minute = (int) $matches[11];
				}
			}
			else if($hasDate) // date only.  set time to midnight like standard
			{
				$hour = 0;
				$minute = 0;
				$second = 0;
				$microsecond = 0;
			}
			
			if(!isset($tz_hour))
			{
				$tz = date('O');
				$tz_hour = (int) substr($tz, 0, 3);
				$tz_minute = (int) substr($tz, 3);
			}
			
			switch($field)
			{
				case 'year':
					if($hasDate)
						return $year;
				case 'month':
					if($hasDate)
						return $month;
				case 'day':
					if($hasDate)
						return $day;
				case 'hour':
					return $hour;
				case 'minute':
					return $minute;
				case 'second':
					return $second;
				case 'microsecond':
					return $microsecond;
				case 'timezone_hour':
					return $tz_hour;
				case 'timezone_minute':
					return $tz_minute;
			}
			
			return null;
		}
	}
	
	function localtime($precision = 6) {
		return $this->_build_time(FSQL_FORMAT_TIME, $precision);
	}
	
	function localtimestamp($precision = 6)
	{
		return $this->_build_time(FSQL_FORMAT_TIMESTAMP, $precision);
	}
}

?>