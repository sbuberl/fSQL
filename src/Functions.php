<?php

namespace FSQL;

class Functions
{
    const REGISTERED = 0;
    const NORMAL = 1;
    const CUSTOM_PARSE = 2;
    const BUILTIN_ID = 4;
    const AGGREGATE = 8;

    private $allowed = array('abs', 'acos', 'asin', 'atan2', 'atan', 'ceil', 'cos', 'crc32', 'exp', 'floor',
       'ltrim', 'md5', 'pi', 'pow', 'rand', 'rtrim', 'round', 'sha1', 'sin', 'soundex', 'sqrt', 'strcmp', 'tan', );

    private $custom = array(
        'any' => self::AGGREGATE,
        'avg' => self::AGGREGATE,
        'cast' => self::CUSTOM_PARSE,
        'concat' => self::NORMAL,
        'concat_ws' => self::NORMAL,
        'count' => self::AGGREGATE,
        'curdate' => self::NORMAL,
        'current_catalog' => self::NORMAL,
        'current_schema' => self::NORMAL,
        'curtime' => self::NORMAL,
        'currval' => self::NORMAL,
        'dayofweek' => self::NORMAL,
        'dayofyear' => self::NORMAL,
        'elt' => self::NORMAL,
        'every' => self::AGGREGATE,
        'extract' => self::CUSTOM_PARSE,
        'from_unixtime' => self::NORMAL,
        'last_insert_id' => self::NORMAL,
        'left' => self::NORMAL,
        'locate' => self::NORMAL,
        'log' => self::NORMAL,
        'log2' => self::NORMAL,
        'log10' => self::NORMAL,
        'lpad' => self::NORMAL,
        'max' => self::AGGREGATE,
        'min' => self::AGGREGATE,
        'mod' => self::NORMAL,
        'month' => self::NORMAL,
        'nextval' => self::NORMAL,
        'now' => self::NORMAL,
        'overlay' => self::CUSTOM_PARSE,
        'position' => self::CUSTOM_PARSE,
        'repeat' => self::NORMAL,
        'right' => self::NORMAL,
        'row_count' => self::NORMAL,
        'sign' => self::NORMAL,
        'substring' => self::CUSTOM_PARSE,
        'substring_index' => self::NORMAL,
        'sum' => self::AGGREGATE,
        'trim' => self::CUSTOM_PARSE,
        'truncate' => self::NORMAL,
        'unix_timestamp' => self::NORMAL,
        'weekday' => self::NORMAL,
        'year' => self::NORMAL,
    );

    private $renamed = array('conv' => 'base_convert', 'ceiling' => 'ceil', 'database' => 'current_catalog', 'degrees' => 'rad2deg',
        'format' => 'number_format', 'length' => 'strlen', 'lower' => 'strtolower', 'ln' => 'log', 'power' => 'pow', 'quote' => 'addslashes',
        'radians' => 'deg2rad', 'repeat' => 'str_repeat', 'replace' => 'strtr', 'reverse' => 'strrev',
        'rpad' => 'str_pad', 'sha' => 'sha1', 'some' => 'any', 'substr' => 'substring', 'upper' => 'strtoupper', );

    private $environment;

    public function __construct(Environment $fsql)
    {
        $this->environment = $fsql;
    }

    public function lookup($function)
    {
        if (isset($this->renamed[$function])) {
            $newName = $this->renamed[$function];
            if (isset($this->custom[$newName])) {
                $type = $this->custom[$newName];
            } else {
                $type = self::REGISTERED;
            }

            return array($newName, $type);
        } elseif (isset($this->custom[$function])) {
            return array($function, $this->custom[$function]);
        } elseif (in_array($function, $this->allowed)) {
            return array($function, self::REGISTERED);
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
        return preg_replace("/^'(.+)'$/s", '\\1', $string);
    }

    public function cast($expression, $type)
    {
        switch ($type) {
            case Types::FLOAT:
                return Types::to_float($expression);
            case Types::INTEGER:
                return Types::to_int($expression);
            case Types::STRING:
                return Types::to_string($expression);
        }

        return false;
    }

    //////Misc Functions
    public function current_catalog()
    {
        $db = $this->environment->current_db();

        return $db !== null ? $db->name() : null;
    }

    public function current_schema()
    {
        $schema = $this->environment->current_schema();

        return $schema !== null ? $schema->name() : null;
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
        $schema = $this->environment->current_schema();
        $sequences = $schema->getSequences();
        $sequence = $sequences->getSequence($sequenceName);

        return $sequence->lastValue();
    }

    public function nextval($sequenceName)
    {
        $schema = $this->environment->current_schema();
        $sequences = $schema->getSequences();
        $sequence = $sequences->getSequence($sequenceName);

        return $sequence->nextValueFor();
    }

    /////Math Functions
    public function log($arg1, $arg2 = null)
    {
        $arg1 = $this->trimQuotes($arg1);
        if ($arg2) {
            $arg2 = $this->trimQuotes($arg2);
        }
        if (($arg1 < 0 || $arg1 == 1) && !$arg2) {
            return;
        }
        if (!$arg2) {
            return log($arg1);
        } else {
            return log($arg2) / log($arg1);
        }
    }

    public function log2($arg)
    {
        $arg = $this->trimQuotes($arg);

        return $this->log(2, $arg);
    }

    public function log10($arg)
    {
        $arg = $this->trimQuotes($arg);

        return $this->log(10, $arg);
    }

    public function mod($one, $two)
    {
        $one = $this->trimQuotes($one);
        $two = $this->trimQuotes($two);

        return $one % $two;
    }

    public function sign($number)
    {
        $number = $this->trimQuotes($number);
        if ($number > 0) {
            return 1;
        } elseif ($number == 0) {
            return 0;
        } else {
            return -1;
        }
    }

    public function truncate($number, $places)
    {
        $number = $this->trimQuotes($number);
        $places = round($this->trimQuotes($places));
        list($integer, $decimals) = explode('.', $number);
        if ($places == 0) {
            return $integer;
        } elseif ($places > 0) {
            return $integer.'.'.substr($decimals, 0, $places);
        } else {
            return substr($number, 0, $places) * pow(10, abs($places));
        }
    }

    ///// Aggregate Functions

    public function any($data, $column, $isConstant)
    {
        if ($isConstant) {
            return Types::isTrue($column);
        }

        foreach ($data as $entry) {
            if (Types::isTrue($entry[$column])) {
                return true;
            }
        }

        return false;
    }

    public function avg($data, $column, $isConstant)
    {
        $sum = $this->sum($data, $column, $isConstant);
        if($sum !== null) {
            if($isConstant) {
                return $sum;
            } else {
                return $sum / count($data);
            }
        }

        return null;
    }

    public function count($data, $column, $isConstant)
    {
        if ($column == '*') {
            return count($data);
        } elseif ($isConstant) {
            return (int) ($column !== null);
        } else {
            $i = 0;
            foreach ($data as $entry) {
                if ($entry[$column] !== null) {
                    ++$i;
                }
            }

            return $i;
        }
    }

    public function every($data, $column, $isConstant)
    {
        if ($isConstant) {
            return Types::isTrue($column);
        }
        foreach ($data as $entry) {
            if (!Types::isTrue($entry[$column])) {
                return false;
            }
        }

        return true;
    }

    public function max($data, $column, $isConstant)
    {
        $max = null;
        if ($isConstant) {
            $max = $column;
        } else {
            foreach ($data as $entry) {
                if ($entry[$column] > $max || $max === null) {
                    $max = $entry[$column];
                }
            }
        }

        return $max;
    }

    public function min($data, $column, $isConstant)
    {
        $min = null;
        if ($isConstant) {
            $min = $column;
        } else {
            foreach ($data as $entry) {
                if ($entry[$column] < $min || $min === null) {
                    $min = $entry[$column];
                }
            }
        }

        return $min;
    }

    public function sum($data, $column, $isConstant)
    {
        $i = null;

        if ($isConstant) {
            $i = $column;
        } else {
            foreach ($data as $entry) {
                $i += $entry[$column];
            }
        }

        return $i;
    }

     /////String Functions
    public function concat_ws($separator, ...$args)
    {
        return implode($separator, $args);
    }

    public function concat(...$args)
    {
        array_unshift($args, '');
        return call_user_func_array(array($this, 'concat_ws'), $args);
    }

    public function elt($index, ...$elements)
    {
        $count = count($elements);
        if ($count > 1 && $index >= 1 && $index <= $count) {
            return $elements[$index];
        } else {
            return;
        }
    }

    public function locate($string, $find, $start = null)
    {
        if ($start) {
            $string = substr($string, $start);
        }
        $pos = strpos($string, $find);
        if ($pos === false) {
            return 0;
        } else {
            return $pos;
        }
    }

    public function lpad($string, $length, $pad)
    {
        return str_pad($string, $length, $pad, STR_PAD_LEFT);
    }

    public function left($string, $end)
    {
        return substr($string, 0, $end);
    }

    public function right($string, $end)
    {
        return substr($string, -$end);
    }

    public function substring_index($string, $delim, $count)
    {
        $array = array();
        $parts = explode($delim, $string);
        if ($count < 0) {
            for ($i = $count; $i > 0; ++$i) {
                $part = count($parts) + $i;
                $array[] = $parts[$part];
            }
        } else {
            for ($i = 0; $i < $count; ++$i) {
                $array[] = $parts[$i];
            }
        }

        return implode($delim, $array);
    }

    // not public
    public function ltrim($string, $charlist = ' ')
    {
        return ($string !== null) ? ltrim($string, $charlist) : null;
    }

    public function overlay($string, $other, $start = 1)
    {
        if ($string !== null && $other !== null) {
            $start -= 1; // one-based not zero-based
            if (func_num_args() === 4) {
                $length = func_get_arg(3);

                return substr_replace($string, $other, $start, $length);
            } else {
                return substr_replace($string, $other, $start);
            }
        }
    }

    public function position($substring, $string)
    {
        if ($string !== null && $substring !== null) {
            $pos = strpos($string, $substring);
            if ($pos !== false) {
                return $pos + 1;
            } else {
                return 0;
            }
        } else {
            return;
        }
    }

    // not public
    public function rtrim($string, $charlist = ' ')
    {
        return ($string !== null) ? rtrim($string, $charlist) : null;
    }

    public function substring($string, $pos)
    {
        if ($string !== null) {
            $pos -= 1; // one-based not zero-based
            if (func_num_args() === 3) {
                $length = func_get_arg(2);

                return substr($string, $pos, $length);
            } else {
                return substr($string, $pos);
            }
        }
    }

    public function trim($string, $charlist = ' ')
    {
        return ($string !== null) ? trim($string, $charlist) : null;
    }

    ////Date/Time functions
    public function now()
    {
        return $this->from_unixtime(time());
    }

    public function curdate()
    {
        return $this->from_unixtime(time(), '%Y-%m-%d');
    }

    public function curtime()
    {
        return $this->from_unixtime(time(), '%H:%M:%S');
    }

    public function dayofweek($date)
    {
        return $this->from_unixtime($date, '%w');
    }

    public function weekday($date)
    {
        return $this->from_unixtime($date, '%u');
    }

    public function dayofyear($date)
    {
        return round($this->from_unixtime($date, '%j'));
    }

    public function unix_timestamp($date = null)
    {
        if (!$date) {
            return;
        } else {
            return strtotime(str_replace('-', '/', $date));
        }
    }

    public function from_unixtime($timestamp, $format = '%Y-%m-%d %H:%M:%S')
    {
        if (!is_int($timestamp)) {
            $timestamp = $this->unix_timestamp($timestamp);
        }

        return strftime($format, $timestamp);
    }

    public function extract($field, $datetime)
    {
        if ($datetime !== null && $field != NULL && preg_match('/\A(((?:[1-9]\d)?\d{2})-(0\d|1[0-2])-([0-2]\d|3[0-1])\s*)?(\b([0-1]\d|2[0-3]):([0-5]\d):([0-5]\d)(?:\.(\d+))?(?:([\+\-]\d{2}):(\d{2}))?)?\Z/is', $datetime, $matches)) {
            $hasDate = !empty($matches[1]);

            if ($hasDate) { // date or timestamp
                $year = (int) $matches[2];
                $month = (int) $matches[3];
                $day = (int) $matches[4];
            }

            if (isset($matches[5])) { // time or timestamp
                $hour = (int) $matches[6];
                $minute = (int) $matches[7];
                $second = (int) $matches[8];
                if (isset($matches[9])) {
                    $microsecond = (int) $matches[9];
                } else {
                    $microsecond = 0;
                }
                if (isset($matches[10])) {
                    $tz_hour = (int) $matches[10];
                    $tz_minute = (int) $matches[11];
                }
            } elseif ($hasDate) { // date only.  set time to midnight like standard
                $hour = 0;
                $minute = 0;
                $second = 0;
                $microsecond = 0;
            }

            if (!isset($tz_hour)) {
                $tz = date('O');
                $tz_hour = (int) substr($tz, 0, 3);
                $tz_minute = (int) substr($tz, 3);
            }

            switch ($field) {
                case 'year':
                    if ($hasDate) {
                        return $year;
                    }
                case 'month':
                    if ($hasDate) {
                        return $month;
                    }
                case 'day':
                    if ($hasDate) {
                        return $day;
                    }
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

            return;
        }
    }
}
