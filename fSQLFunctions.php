<?php

define('FSQL_FUNC_REGISTERED', 0, true);
define('FSQL_FUNC_NORMAL', 1, true);
define('FSQL_FUNC_AGGREGATE', 2, true);

class fSQLFunctions
{
	var $allowed = array('abs','acos','asin','atan2','atan','ceil','cos','crc32','exp','floor',
	   'ltrim','md5','pi','pow','rand','rtrim','round','sha1','sin','soundex','sqrt','strcmp','tan');
	var $aggregates = array('any','avg','count','every','max','min','sum');
	var $custom = array('concat','concat_ws','curdate','curtime','database','dayofweek',
	   'dayofyear','elt','from_unixtime','last_insert_id', 'left','locate','log','log2','log10','lpad',
	   'mod','month','now','repeat','right','row_count','sign','substring_index','truncate','unix_timestamp',
	   'weekday','year');
	var $renamed = array('conv'=>'base_convert','ceiling' => 'ceil','degrees'=>'rad2deg','format'=>'number_format',
	   'length'=>'strlen','lower'=>'strtolower','ln'=>'log','power'=>'pow','quote'=>'addslashes',
	   'radians'=>'deg2rad','repeat'=>'str_repeat','replace'=>'strtr','reverse'=>'strrev',
	   'rpad'=>'str_pad','sha' => 'sha1','some' => 'any','substring'=>'substr','upper'=>'strtoupper');

	function fSQLFunctions()
	{
	}

	function close()
	{
		unset($this->allowed, $this->aggregates, $this->custom, $this->renamed);
	}

	function lookup($function)
	{
		if(isset($this->renamed[$function])) {
			$newName = $this->renamed[$function];
			if(in_array($newName, $this->custom)){
				$type = FSQL_FUNC_NORMAL;
			} else if(in_array($newName, $this->aggregates)){
				$type = FSQL_FUNC_AGGREGATE | FSQL_FUNC_NORMAL;
			} else {
				$type = FSQL_TYPE_REGISTERED;
			}
			return array($newName, $type);
		} else if(in_array($function, $this->aggregates)) {
			return array($function, FSQL_FUNC_AGGREGATE | FSQL_FUNC_NORMAL);
		} else if(in_array($function, $this->custom)) {
			return array($function, FSQL_FUNC_NORMAL);
		} else if(in_array($function, $this->allowed)) {
			return array($function, FSQL_FUNC_REGISTERED);
		} else {
			return false;
		}
	}

	function register($sqlName, $phpName)
	{
		$this->renamed_func[$sqlName] = $phpName;
		return true;
	}

	function _trimQuotes($string)
	{
		return preg_replace("/^'(.+)'$/s", "\\1", $string);
	}

		// operators

	function not($x)
	{
		$c = ~$x & 3;
		return (($c << 1) ^ ($c >> 1)) & 3;
	}

	function isTrue($expr)
	{
		return !in_array($expr, array(0, 0.0, '', null), true);
	}

	function isFalse($expr)
	{
		return in_array($expr, array(0, 0.0, ''), true);
	}

	function like($left, $right)
	{
		if($left !== null && $right !== null)
		{
			$right = strtr(preg_quote($right, "/"), array('_' => '.', '%' => '.*', '\_' => '_', '\%' => '%'));
			return (preg_match("/\A{$right}\Z/is", $left)) ? FSQL_TRUE : FSQL_FALSE;
		}
		else
			return FSQL_UNKNOWN;
	}

	function in($needle, $haystack)
	{
		if($needle !== null)
		{
			return (in_array($needle, $haystack)) ? FSQL_TRUE : FSQL_FALSE;
		}
		else
			return FSQL_UNKNOWN;
	}

	function regexp($left, $right)
	{
		if($left !== null && $right !== null)
			return (preg_match('/'.$right.'/i', $left)) ? FSQL_TRUE : FSQL_FALSE;
		else
			return FSQL_UNKNOWN;
	}

	//////Misc Functions
	function database()
	{
		return $this->currentDB->name;
	}

	function last_insert_id()
	{
		return $this->insert_id;
	}

	function row_count()
	{
		return $this->affected;
	}

	/////Math Functions
	function log($arg1, $arg2 = null) {
		$arg1 = $this->_trimQuotes($arg1);
		if($arg2) {
			$arg2 = $this->_trimQuotes($arg2);
		}
		if(($arg1 < 0 || $arg1 == 1) && !$arg2) { return null; }
		if(!$arg2) { return log($arg1); } else { return log($arg2) / log($arg1); }
	}
	function log2($arg)
	{
		$arg = $this->_trimQuotes($arg);
		return $this->log(2, $arg);
	}
	function log10($arg) {
		$arg = $this->_trimQuotes($arg);
		return $this->log(10, $arg);
	}
	function mod($one, $two) {
		$one = $this->_trimQuotes($one);
		$two = $this->_trimQuotes($two);
		return $one % $two;
	}
	function sign($number) {
		$number = $this->_trimQuotes($number);
		if($number > 0) { return 1; } else if($number == 0) { return 0; } else { return -1; }
	}
	function truncate($number, $places) {
		$number = $this->_trimQuotes($number);
		$places = round($this->_trimQuotes($number));
		list($integer, $decimals) = explode(".", $number);
		if($places == 0) { return $integer; }
		else if($places > 0) { return $integer.'.'.substr($decimals,0,$places); }
		else {   return substr($number,0,$places) * pow(10, abs($places));  }
	}

	 ///// Aggregate Functions
	function any($data, $column, $flag) {
		if ($flag === "constant")
			return $this->isTrue($data);
		foreach($data as $entry){
			if($this->isTrue($entry[$column]))
				return true;
		}
		return false;
	}

	function avg($data, $column, $flag) {
		$sum = $this->sum($data, $column, $flag);
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
			return $this->isTrue($data);
		foreach($data as $entry){
			if(!$this->isTrue($entry[$column]))
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
	function concat_ws($string) {
		$numargs = func_num_args();
		if($numargs > 2) {
			for($i = 1; $i < $numargs; $i++) { $return[] = func_get_arg($i);  }
			return implode($string, $return);
		}
		else { return null; }
	}
	function concat() { return call_user_func_array(array($this,'concat_ws'), array("",func_get_args())); }
	function elt() {
		$return = func_get_arg(0);
		if(func_num_args() > 1 && $return >= 1 && $return <= func_num_args()) {	return func_get_arg($return);  }
		else { return null; }
	}
	function locate($string, $find, $start = null) {
		if($start) { $string = substr($string, $start); }
		$pos = strpos($string, $find);
		if($pos === false) { return 0; } else { return $pos; }
	}
	function lpad($string, $length, $pad) { return str_pad($string, $length, $pad, STR_PAD_LEFT); }
	function left($string, $end)	{ return substr($string, 0, $end); }
	function right($string,$end)	{ return substr($string, -$end); }
	function substring_index($string, $delim, $count) {
		$parts = explode($delim, $string);
		if($count < 0) {   for($i = $count; $i > 0; $i++) { $part = count($parts) + $i; $array[] = $parts[$part]; }  }
		else { for($i = 0; $i < $count; $i++) { $array[] = $parts[$i]; }  }
		return implode($delim, $array);
	}

	////Date/Time functions
	function now()		{ return $this->from_unixtime(time()); }
	function curdate()	{ return $this->from_unixtime(time(), "%Y-%m-%d"); }
	function curtime() 	{ return $this->from_unixtime(time(), "%H:%M:%S"); }
	function dayofweek($date) 	{ return $this->from_unixtime($date, "%w"); }
	function weekday($date)		{ return $this->from_unixtime($date, "%u"); }
	function dayofyear($date)		{ return round($this->from_unixtime($date, "%j")); }
	function unix_timestamp($date = null) {
		if(!$date) { return null; } else { return strtotime(str_replace("-","/",$date)); }
	}
	function from_unixtime($timestamp, $format = "%Y-%m-%d %H:%M:%S")
	{
		if(!is_int($timestamp)) { $timestamp = $this->unix_timestamp($timestamp); }
		return strftime($format, $timestamp);
	}
}

?>
