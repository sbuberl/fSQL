<?php

define('FSQL_FUNC_REGISTERED', 0);
define('FSQL_FUNC_NORMAL', 1);
define('FSQL_FUNC_CUSTOM_PARSE', 2);
define('FSQL_FUNC_BUILTIN_ID', 4);
define('FSQL_FUNC_AGGREGATE', 8);

class fSQLFunctions
{
    private $allowed = array('abs','acos','asin','atan2','atan','ceil','cos','crc32','exp','floor',
       'ltrim','md5','pi','pow','rand','rtrim','round','sha1','sin','soundex','sqrt','strcmp','tan');

    private $custom = array(
        'any' => FSQL_FUNC_AGGREGATE,
        'avg' => FSQL_FUNC_AGGREGATE,
        'concat' => FSQL_FUNC_NORMAL,
        'concat_ws' => FSQL_FUNC_NORMAL,
        'count' => FSQL_FUNC_AGGREGATE,
        'curdate' => FSQL_FUNC_NORMAL,
        'curtime' => FSQL_FUNC_NORMAL,
        'currval' => FSQL_FUNC_NORMAL,
        'database' =>  FSQL_FUNC_NORMAL,
        'dayofweek' => FSQL_FUNC_NORMAL,
        'dayofyear' => FSQL_FUNC_NORMAL,
        'elt' => FSQL_FUNC_NORMAL,
        'every' => FSQL_FUNC_AGGREGATE,
        'extract' => FSQL_FUNC_CUSTOM_PARSE,
        'from_unixtime' => FSQL_FUNC_NORMAL,
        'last_insert_id' => FSQL_FUNC_NORMAL,
        'left' => FSQL_FUNC_NORMAL,
        'locate' => FSQL_FUNC_NORMAL,
        'log' => FSQL_FUNC_NORMAL,
        'log2' => FSQL_FUNC_NORMAL,
        'log10' => FSQL_FUNC_NORMAL,
        'lpad' => FSQL_FUNC_NORMAL,
        'max' => FSQL_FUNC_AGGREGATE,
        'min' => FSQL_FUNC_AGGREGATE,
        'mod' => FSQL_FUNC_NORMAL,
        'month' => FSQL_FUNC_NORMAL,
        'nextval' => FSQL_FUNC_NORMAL,
        'now' => FSQL_FUNC_NORMAL,
        'overlay' => FSQL_FUNC_CUSTOM_PARSE,
        'position' => FSQL_FUNC_CUSTOM_PARSE,
        'repeat' => FSQL_FUNC_NORMAL,
        'right' => FSQL_FUNC_NORMAL,
        'row_count' => FSQL_FUNC_NORMAL,
        'sign' => FSQL_FUNC_NORMAL,
        'substring' => FSQL_FUNC_CUSTOM_PARSE,
        'substring_index' => FSQL_FUNC_NORMAL,
        'sum' => FSQL_FUNC_AGGREGATE,
        'trim' => FSQL_FUNC_CUSTOM_PARSE,
        'truncate' => FSQL_FUNC_NORMAL,
        'unix_timestamp' => FSQL_FUNC_NORMAL,
        'weekday' => FSQL_FUNC_NORMAL,
        'year' => FSQL_FUNC_NORMAL
    );

    private $renamed = array('conv'=>'base_convert','ceiling' => 'ceil','degrees'=>'rad2deg','format'=>'number_format',
       'length'=>'strlen','lower'=>'strtolower','ln'=>'log','power'=>'pow','quote'=>'addslashes',
       'radians'=>'deg2rad','repeat'=>'str_repeat','replace'=>'strtr','reverse'=>'strrev',
       'rpad'=>'str_pad','sha' => 'sha1','some' => 'any','substr'=>'substring','upper'=>'strtoupper');

    private $environment;

    public function __construct(fSQLEnvironment $fsql)
    {
        $this->environment = $fsql;
    }

    public function lookup($function)
    {
        if(isset($this->renamed[$function])) {
            $newName = $this->renamed[$function];
            if(isset($this->custom[$newName])){
                $type = $this->custom[$newName];
            } else {
                $type = FSQL_FUNC_REGISTERED;
            }
            return array($newName, $type);
        } else if(isset($this->custom[$function])) {
            return array($function, $this->custom[$function]);
        } else if(in_array($function, $this->allowed)) {
            return array($function, FSQL_FUNC_REGISTERED);
        } else {
            return false;
        }
    }

    public function register($sqlName, $phpName)
    {
        $this->renamed[$sqlName] = $phpName;
        return true;
    }

    private function trimQuotes($string)
    {
        return preg_replace("/^'(.+)'$/s", "\\1", $string);
    }

    // operators

    public function not($x)
    {
        $c = ~$x & 3;
        return (($c << 1) ^ ($c >> 1)) & 3;
    }

    public function isTrue($expr)
    {
        return !in_array($expr, array(0, 0.0, '', null), true);
    }

    public function isFalse($expr)
    {
        return in_array($expr, array(0, 0.0, ''), true);
    }

    public function like($left, $right)
    {
        if($left !== null && $right !== null)
        {
            $right = strtr(preg_quote($right, "/"), array('_' => '.', '%' => '.*', '\_' => '_', '\%' => '%'));
            return (preg_match("/\A{$right}\Z/is", $left)) ? FSQL_TRUE : FSQL_FALSE;
        }
        else
            return FSQL_UNKNOWN;
    }

    public function in($needle, $haystack)
    {
        if($needle !== null)
        {
            return (in_array($needle, $haystack)) ? FSQL_TRUE : FSQL_FALSE;
        }
        else
            return FSQL_UNKNOWN;
    }

    public function regexp($left, $right)
    {
        if($left !== null && $right !== null)
            return (preg_match('/'.$right.'/i', $left)) ? FSQL_TRUE : FSQL_FALSE;
        else
            return FSQL_UNKNOWN;
    }

    //////Misc Functions
    public function database()
    {
        return $this->environment->current_db()->name();
    }

    public function last_insert_id()
    {
        return $this->environment->insert_id();
    }

    public function row_count()
    {
        return $this->environment->affected_rows();
    }

    /////Sequence Functions
    public function currval($sequenceName)
    {
        $db = $this->environment->current_db();
        $sequences = $db->getSequences();
        $sequence = $sequences->getSequence($sequenceName);
        return $sequence->lastValue();
    }

    public function nextval($sequenceName)
    {
        $db = $this->environment->current_db();
        $sequences = $db->getSequences();
        $sequence = $sequences->getSequence($sequenceName);
        return $sequence->nextValueFor();
    }

    /////Math Functions
    public function log($arg1, $arg2 = null) {
        $arg1 = $this->trimQuotes($arg1);
        if($arg2) {
            $arg2 = $this->trimQuotes($arg2);
        }
        if(($arg1 < 0 || $arg1 == 1) && !$arg2) { return null; }
        if(!$arg2) { return log($arg1); } else { return log($arg2) / log($arg1); }
    }
    public function log2($arg)
    {
        $arg = $this->trimQuotes($arg);
        return $this->log(2, $arg);
    }
    public function log10($arg) {
        $arg = $this->trimQuotes($arg);
        return $this->log(10, $arg);
    }
    public function mod($one, $two) {
        $one = $this->trimQuotes($one);
        $two = $this->trimQuotes($two);
        return $one % $two;
    }
    public function sign($number) {
        $number = $this->trimQuotes($number);
        if($number > 0) { return 1; } else if($number == 0) { return 0; } else { return -1; }
    }
    public function truncate($number, $places) {
        $number = $this->trimQuotes($number);
        $places = round($this->trimQuotes($places));
        list($integer, $decimals) = explode(".", $number);
        if($places == 0) { return $integer; }
        else if($places > 0) { return $integer.'.'.substr($decimals,0,$places); }
        else {   return substr($number,0,$places) * pow(10, abs($places));  }
    }

     ///// Aggregate Functions
    public function any($data, $column, $flag) {
        if ($flag === "constant")
            return $this->isTrue($data);
        foreach($data as $entry){
            if($this->isTrue($entry[$column]))
                return true;
        }
        return false;
    }

    public function avg($data, $column, $flag) {
        $sum = $this->sum($data, $column, $flag);
        return $sum !== null ? $sum / count($data) : null;
    }

    public function count($data, $column, $flag) {
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

    public function every($data, $column, $flag) {
        if ($flag === "constant")
            return $this->isTrue($data);
        foreach($data as $entry){
            if(!$this->isTrue($entry[$column]))
                return false;
        }
        return true;
    }

    public function max($data, $column, $flag) {
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

    public function min($data, $column, $flag) {
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

    public function sum($data, $column, $flag) {
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
    public function concat_ws($string) {
        $numargs = func_num_args();
        if($numargs > 2) {
            for($i = 1; $i < $numargs; $i++) { $return[] = func_get_arg($i);  }
            return implode($string, $return);
        }
        else { return null; }
    }
    public function concat() { return call_user_func_array(array($this,'concat_ws'), array("",func_get_args())); }
    public function elt() {
        $return = func_get_arg(0);
        if(func_num_args() > 1 && $return >= 1 && $return <= func_num_args()) {    return func_get_arg($return);  }
        else { return null; }
    }
    public function locate($string, $find, $start = null) {
        if($start) { $string = substr($string, $start); }
        $pos = strpos($string, $find);
        if($pos === false) { return 0; } else { return $pos; }
    }
    public function lpad($string, $length, $pad) { return str_pad($string, $length, $pad, STR_PAD_LEFT); }
    public function left($string, $end)    { return substr($string, 0, $end); }
    public function right($string,$end)    { return substr($string, -$end); }
    public function substring_index($string, $delim, $count) {
        $parts = explode($delim, $string);
        if($count < 0) {   for($i = $count; $i > 0; $i++) { $part = count($parts) + $i; $array[] = $parts[$part]; }  }
        else { for($i = 0; $i < $count; $i++) { $array[] = $parts[$i]; }  }
        return implode($delim, $array);
    }

        // not public
    public function ltrim($string, $charlist = ' ') {
        return ($string !== null) ? ltrim($string, $charlist) : null;
    }

    public function overlay($string, $other, $start = 1) {
        if($string !== null && $other !== null) {
            $start -= 1; // one-based not zero-based
            if(func_num_args() === 4) {
                $length = func_get_arg(3);
                return substr_replace($string, $other, $start, $length);
            }
            else
                return substr_replace($string, $other, $start);
        }

        return null;
    }

    public function position($substring, $string) {
        if($string !== null && $substring !== null) {
            $pos = strpos($string, $substring);
            if($pos !== false) { return $pos + 1; } else { return 0; }
        }
        else
            return null;
    }

    // not public
    public function rtrim($string, $charlist = ' ') {
        return ($string !== null) ? rtrim($string, $charlist) : null;
    }

    public function substring($string, $pos) {
        if($string !== null) {
            $pos -= 1; // one-based not zero-based
            if(func_num_args() === 3) {
                $length = func_get_arg(2);
                return substr($string, $pos, $length);
            }
            else
                return substr($string, $pos);
        }

        return null;
    }

    public function trim($string, $charlist = ' ') {
        return ($string !== null) ? trim($string, $charlist) : null;
    }

    ////Date/Time functions
    public function now()        { return $this->from_unixtime(time()); }
    public function curdate()    { return $this->from_unixtime(time(), "%Y-%m-%d"); }
    public function curtime()     { return $this->from_unixtime(time(), "%H:%M:%S"); }
    public function dayofweek($date)     { return $this->from_unixtime($date, "%w"); }
    public function weekday($date)        { return $this->from_unixtime($date, "%u"); }
    public function dayofyear($date)        { return round($this->from_unixtime($date, "%j")); }
    public function unix_timestamp($date = null) {
        if(!$date) { return null; } else { return strtotime(str_replace("-","/",$date)); }
    }
    public function from_unixtime($timestamp, $format = "%Y-%m-%d %H:%M:%S")
    {
        if(!is_int($timestamp)) { $timestamp = $this->unix_timestamp($timestamp); }
        return strftime($format, $timestamp);
    }

    public function extract($field, $datetime)
    {
        if($datetime !== null && preg_match('/\A(((?:[1-9]\d)?\d{2})-(0\d|1[0-2])-([0-2]\d|3[0-1])\s*)?(\b([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)(?:\.(\d+))?(?:([\+\-]\d{2}):(\d{2}))?)?\Z/is', $datetime, $matches)) {
            $hasDate = !empty($matches[1]);

            if($hasDate) { // date or timestamp
                $year = (int) $matches[2];
                $month = (int) $matches[3];
                $day = (int) $matches[4];
            }

            if(isset($matches[5])) { // time or timestamp
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
            } else if($hasDate) { // date only.  set time to midnight like standard
                $hour = 0;
                $minute = 0;
                $second = 0;
                $microsecond = 0;
            }

            if(!isset($tz_hour)) {
                $tz = date('O');
                $tz_hour = (int) substr($tz, 0, 3);
                $tz_minute = (int) substr($tz, 3);
            }

            switch($field) {
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
}
