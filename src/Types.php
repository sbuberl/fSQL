<?php

namespace FSQL;

final class Types
{
    const DATE = 'd';
    const DATETIME = 'dt';
    const ENUM = 'e';
    const FLOAT = 'f';
    const INTEGER = 'i';
    const STRING = 's';
    const TIME = 't';

    private static $TYPES = [
        'CHAR' => self::STRING,
        'VARCHAR' => self::STRING,
        'TEXT' => self::STRING,
        'TINYTEXT' => self::STRING,
        'MEDIUMTEXT' => self::STRING,
        'LONGTEXT' => self::STRING,

        'BINARY' => self::STRING,
        'BLOB' => self::STRING,
        'VARBINARY' => self::STRING,
        'TINYBLOB' => self::STRING,
        'MEDIUMBLOB' => self::STRING,
        'LONGBLOB' => self::STRING,

        'BIT' => self::INTEGER,
        'TINYINT' => self::INTEGER,
        'SMALLINT' => self::INTEGER,
        'MEDIUMINT' => self::INTEGER,
        'INT' => self::INTEGER,
        'INTEGER' => self::INTEGER,
        'BIGINT' => self::INTEGER,

        'FLOAT' => self::FLOAT,
        'REAL' => self::FLOAT,
        'DOUBLE' => self::FLOAT,
        'DOUBLE PRECISION' => self::FLOAT,
        'NUMERIC' => self::FLOAT,
        'DEC' => self::FLOAT,
        'DECIMAL' => self::FLOAT,

        'ENUM' => self::ENUM,

        'DATE' => self::DATE,
        'TIME'  => self::TIME,
        'DATETIME' => self::DATETIME
    ];

    /**
     * @codeCoverageIgnore
     */
    private function __construct()
    {
    }

    public static function getTypeCode($type)
    {
        $type = preg_replace('/\s+/', ' ', strtoupper($type));
		return isset(self::$TYPES[$type]) ? self::$TYPES[$type] : false;
    }

    public static function getTypeName($type)
    {
        switch ($type) {
            case self::DATE:                return 'DATE';
            case self::DATETIME:            return 'DATETIME';
            case self::ENUM:                return 'ENUM';
            case self::FLOAT:               return 'DOUBLE';
            case self::INTEGER:             return 'INTEGER';
            case self::STRING:              return 'TEXT';
            case self::TIME:                return 'TIME';
            default:                        return false;
        }
    }

    public static function to_float($arg) {
        if($arg === null)
            return null;
        elseif(!is_numeric($arg))
            return false;
        return (float) $arg;
    }

    public static function to_int($arg) {
        if($arg === null)
            return null;
        elseif(!is_numeric($arg))
            return false;
        return !is_int($arg) ? (int) $arg : $arg;
    }

    public static function to_string($arg) {
        if($arg === null)
            return null;
        return !is_string($arg) ? (string) $arg : $arg;
    }

    // operators

    public static function not($x)
    {
        $c = ~$x & 3;

        return (($c << 1) ^ ($c >> 1)) & 3;
    }

    public static function isTrue($expr)
    {
        return !in_array($expr, array(0, 0.0, '', null), true);
    }

    public static function isFalse($expr)
    {
        return in_array($expr, array(0, 0.0, ''), true);
    }

    public static function like($left, $right)
    {
        if ($left !== null && $right !== null) {
            $right = strtr(preg_quote($right, '/'), array('_' => '.', '%' => '.*', '\_' => '_', '\%' => '%'));

            return (preg_match("/\A{$right}\Z/is", $left)) ? FSQL_TRUE : FSQL_FALSE;
        } else {
            return FSQL_UNKNOWN;
        }
    }

    public static function in($needle, $haystack)
    {
        if ($needle !== null) {
            return (in_array($needle, $haystack)) ? FSQL_TRUE : FSQL_FALSE;
        } else {
            return FSQL_UNKNOWN;
        }
    }

    public static function regexp($left, $right)
    {
        if ($left !== null && $right !== null) {
            return (preg_match('/'.$right.'/i', $left)) ? FSQL_TRUE : FSQL_FALSE;
        } else {
            return FSQL_UNKNOWN;
        }
    }
}
