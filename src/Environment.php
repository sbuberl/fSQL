<?php

namespace FSQL;

define('FSQL_TRUE', 3);
define('FSQL_FALSE', 0);
define('FSQL_NULL', 1);
define('FSQL_UNKNOWN', 1);

if (!defined('PHP_INT_MIN')) {
    define('PHP_INT_MIN', ~PHP_INT_MAX);
}

define('FSQL_INCLUDE_PATH', __DIR__);

require 'vendor/autoload.php';

use FSQL\Database\Database;
use FSQL\Database\Sequence;
use FSQL\Database\Table;
use FSQL\Database\Transaction;
use FSQL\Statements\CreateTableLike;

class Environment
{
    private static $WHERE_NORMAL = 2;
    private static $WHERE_NORMAL_AGG = 3;
    private static $WHERE_ON = 4;
    private static $WHERE_HAVING = 8;
    private static $WHERE_HAVING_AGG = 9;

    private $transaction = null;
    private $lockedTables = array();
    private $databases = array();
    private $currentDB = null;
    private $currentSchema = null;
    private $error_msg = null;
    private $query_count = 0;
    private $join_lambdas = array();
    private $affected = 0;
    private $insert_id = 0;
    private $auto = true;
    private $functions;

    public function __construct()
    {
        $this->functions = new Functions($this);
    }

    public function __destruct()
    {
        $this->unlock_tables();
    }

    public function define_db($name, $path)
    {
        list($usec, $sec) = explode(' ', microtime());
        srand((float) $sec + ((float) $usec * 100000));

        $db = new Database($this, $name, $path);
        if ($db->create()) {
            $this->databases[$name] = $db;

            return true;
        } else {
            return false;
        }
    }

    public function select_db($name)
    {
        return $this->select_schema($name, 'public');
    }

    public function select_schema($db_name, $schema_name)
    {
        if (isset($this->databases[$db_name])) {
            $db = $this->databases[$db_name];
            $schema = $db->getSchema($schema_name);
            if ($schema !== false) {
                $this->currentDB = $db;
                $this->currentSchema = $schema;

                return true;
            } else {
                return $this->error_schema_not_exist($db_name, $schema_name);
            }
        } else {
            return $this->set_error("No database called {$db_name} found");
        }
    }

    public function current_db()
    {
        return $this->currentDB;
    }

    public function current_schema()
    {
        return $this->currentSchema;
    }

    public function error()
    {
        return $this->error_msg;
    }

    public function register_function($sqlName, $phpName)
    {
        return $this->functions->register($sqlName, $phpName);
    }

    public function set_error($error)
    {
        $this->error_msg = $error."\r\n";

        return false;
    }

    private function build_relation_name($table_name_pieces)
    {
        list($db_name, $schema_name, $table_name) = $table_name_pieces;
        if ($db_name === null) {
            $db_name = $this->currentDB->name();
        }
        if ($schema_name === null) {
            $schema_name = $this->currentSchema !== null ? $this->currentSchema->name() : 'public';
        }

        return $db_name.'.'.$schema_name.'.'.$table_name;
    }

    private function error_schema_not_exist($db_name, $schema_name)
    {
        return $this->set_error("Schema {$db_name}.{$schema_name} does not exist");
    }

    private function error_relation_not_exists($relation_name_pieces, $relation_type)
    {
        $relation_name = $this->build_relation_name($relation_name_pieces);

        return $this->set_error("${relation_type} {$relation_name} does not exist");
    }

    private function error_table_not_exists($table_name_pieces)
    {
        return $this->error_relation_not_exists($table_name_pieces, 'Table');
    }

    private function error_table_read_lock($table_name_pieces)
    {
        $table_name = $this->build_relation_name($table_name_pieces);

        return $this->set_error("Table {$table_name} is locked for reading only");
    }

    public function list_dbs()
    {
        return array_keys($this->databases);
    }

    public function get_database($db_name)
    {
        $db = false;

        if (!$db_name) {
            if ($this->currentDB !== null) {
                $db = $this->currentDB;
            } else {
                $this->set_error('No database specified');
            }
        } else {
            // if database $db_name is not defined set error
            if (isset($this->databases[$db_name])) {
                $db = $this->databases[$db_name];
            } else {
                $this->set_error("Database $db_name not found");
            }
        }

        return $db;
    }

    public function find_schema($db_name, $schema_name)
    {
        $schema = false;

        $db = $this->get_database($db_name);
        if ($db !== false) {
            // if $schema_name is not given try to find current schema
            // else ( $schema_name is given) try to find schema.
            if (!$schema_name) {
                if ($this->currentSchema !== null) {
                    $schema = $this->currentSchema;
                } else {
                    $this->set_error('No schema selected');
                }
            } else {
                $schema = $db->getSchema($schema_name);
                if ($schema === false) {
                    $this->error_schema_not_exist($db->name(), $schema_name);
                }
            }
        }

        return $schema;
    }

    public function find_relation($name_pieces)
    {
        $relation = false;

        if ($name_pieces !== false) {
            list($db_name, $schema_name, $relation_name) = $name_pieces;
            $schema = $this->find_schema($db_name, $schema_name);
            if ($schema) {
                $relation = $schema->getRelation($relation_name);
                if (!$relation) {
                    $this->error_relation_not_exists($name_pieces, 'Relation');
                }
            }
        }

        return $relation;
    }

    public function find_table($name_pieces)
    {
        $table = $this->find_relation($name_pieces);
        if ($table !== false) {
            if (!($table instanceof Table)) {
                $name = $this->build_relation_name($name_pieces);

                return $this->set_error("Relation {$name} is not a table");
            }
        }

        return $table;
    }

    public function parse_relation_name($name)
    {
        if (preg_match('/^(?:(`?)([^\W\d]\w*)\1\.)?(?:(`?)([^\W\d]\w*)\3\.)?(`?)([^\W\d]\w*)\5$/', $name, $matches)) {
            if (!empty($matches[2]) && empty($matches[4])) {
                $db_name = null;
                $schema_name = $matches[2];
            } elseif (empty($matches[2])) {
                $db_name = null;
                $schema_name = null;
            } else {
                $db_name = $matches[2];
                $schema_name = $matches[4];
            }

            return array($db_name, $schema_name, $matches[6]);
        } else {
            return $this->set_error('Parse error in table name: '.$name);
        }
    }

    public function lookup_function($function)
    {
        $match = $this->functions->lookup($function);
        if ($match === false) {
            return $this->set_error('Call to unknown SQL function');
        }

        return $match;
    }

    public function escape_string($string)
    {
        return str_replace(array('\\', "\0", "\n", "\r", "\t", "'"), array('\\\\', '\\0', '\\n', '\\', '\\t', "\\'"), $string);
    }

    public function affected_rows()
    {
        return $this->affected;
    }

    public function insert_id()
    {
        return $this->insert_id;
    }

    public function query_count()
    {
        return $this->query_count;
    }

    private function unlock_tables()
    {
        foreach (array_keys($this->lockedTables) as $index) {
            $this->lockedTables[$index]->unlock();
        }
        $this->lockedTables = array();
    }

    public function is_auto_commit()
    {
        return $this->auto;
    }

    public function auto_commit($auto)
    {
        $this->auto = (bool) $auto;
    }

    public function get_transaction()
    {
        return $this->transaction;
    }

    public function begin()
    {
        // commit any current transaction
        if($this->transaction !== null) {
            $this->transaction->commit();
            $this->unlock_tables();
        }

        $this->transaction = new Transaction($this);
        return $this->transaction->begin();
    }

    public function commit()
    {
        if($this->transaction !== null) {
            $success = $this->transaction->commit();
            $this->transaction = null;
            return $success;
        } else {
            return $this->set_error('Can commit because not inside a transaction');
        }
    }

    public function rollback()
    {
        if($this->transaction !== null) {
            $success = $this->transaction->rollback();
            $this->transaction = null;
            return $success;
        } else {
            return $this->set_error('Can rollback because not inside a transaction');
        }
    }

    public function query($query)
    {
        $query = trim($query);
        $function = strstr($query, ' ', true);
        ++$this->query_count;
        $this->error_msg = null;
        switch (strtoupper($function)) {
            case 'CREATE':      return $this->query_create($query);
            case 'SELECT':      return $this->query_select($query);
            case 'INSERT':
            case 'REPLACE':     return $this->query_insert($query);
            case 'UPDATE':      return $this->query_update($query);
            case 'ALTER':       return $this->query_alter($query);
            case 'DELETE':      return $this->query_delete($query);
            case 'BEGIN':       return $this->query_begin($query);
            case 'START':       return $this->query_start($query);
            case 'COMMIT':      return $this->query_commit($query);
            case 'ROLLBACK':    return $this->query_rollback($query);
            case 'RENAME':      return $this->query_rename($query);
            case 'TRUNCATE':    return $this->query_truncate($query);
            case 'DROP':        return $this->query_drop($query);
            case 'USE':         return $this->query_use($query);
            case 'DESC':
            case 'DESCRIBE':    return $this->query_describe($query);
            case 'SHOW':        return $this->query_show($query);
            case 'LOCK':        return $this->query_lock($query);
            case 'UNLOCK':      return $this->query_unlock($query);
            case 'MERGE':       return $this->query_merge($query);
            default:            return $this->set_error('Invalid Query');
        }
    }

    private function query_begin($query)
    {
        if (preg_match("/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is", $query, $matches)) {
            $this->begin();
            return true;
        } else {
            return $this->set_error('Invalid BEGIN query');
        }
    }

    private function query_start($query)
    {
        if (preg_match("/\ASTART\s+TRANSACTION\s*[;]?\Z/is", $query, $matches)) {
            $this->begin();
            return true;
        } else {
            return $this->set_error('Invalid START query');
        }
    }

    private function query_commit($query)
    {
        if (preg_match("/\ACOMMIT(?:\s+WORK)?\s*[;]?\Z/is", $query, $matches)) {
            $this->commit();
            return true;
        } else {
            return $this->set_error('Invalid COMMIT query');
        }
    }

    private function query_rollback($query)
    {
        if (preg_match("/\AROLLBACK(?:\s+WORK)?\s*[;]?\Z/is", $query, $matches)) {
            $this->rollback();
            return true;
        } else {
            return $this->set_error('Invalid ROLLBACK query');
        }
    }

    private function query_create($query)
    {
        if (preg_match("/\ACREATE\s+((?:TEMPORARY\s+)?TABLE|(?:S(?:CHEMA|EQUENCE)))\s+(?:(IF\s+NOT\s+EXISTS)\s+)?(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $type, $ifNotExists, $definition) = $matches;
            $type = strtoupper($type);
            $ifNotExists = !empty($ifNotExists);
            if (substr($type, -5) === 'TABLE') {
                $temp = !strncmp($type, 'TEMPORARY', 9);

                $query = $this->query_create_table($definition, $temp, $ifNotExists);
            } elseif ($type === 'SCHEMA') {
                $query = $this->query_create_schema($definition, $ifNotExists);
            } else {
                $query = $this->query_create_sequence($definition, $ifNotExists);
            }

            if ($query !== false) {
                return $query->execute();
            } else {
                return false;
            }
        } else {
            return $this->set_error('Invalid CREATE query');
        }
    }

    private function query_create_schema($definition, $ifNotExists)
    {
        if (preg_match("/\A(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?\Z/is", $definition, $matches)) {
            list(, $dbName, $schemaName) = $matches;

            return new Statements\CreateSchema($this, array($dbName, $schemaName), $ifNotExists);
        } else {
            return $this->set_error('Invalid CREATE SCHEMA query');
        }
    }

    private function query_create_sequence($definition, $ifNotExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.+)\Z/is", $definition, $matches)) {
            list(, $fullSequenceName, $valuesList) = $matches;
            $seqNamePieces = $this->parse_relation_name($fullSequenceName);
            if ($seqNamePieces === false) {
                return false;
            }

            $parsed = $this->parse_sequence_options($valuesList);
            if ($parsed === false) {
                return false;
            }

            $initialValues = $this->load_create_sequence($parsed);

            return new Statements\CreateSequence($this, $seqNamePieces, $ifNotExists, $initialValues);
        } else {
            return $this->set_error('Invalid CREATE SEQUENCE query');
        }
    }

    private function query_create_table($definition, $temporary, $ifNotExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s*\((.+)\)|\s+LIKE\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+([\w\s}]+))?)\s*[;]?/is", $definition, $matches)) {
            list(, $full_table_name, $column_list) = $matches;

            $table_name_pieces = $this->parse_relation_name($full_table_name);
            if ($table_name_pieces === false) {
                return false;
            }

            if (!isset($matches[3])) {
                preg_match_all("/(?:(?:CONSTRAINT\s+(?:`?[^\W\d]\w*`?\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+`?([^\W\d]\w*)`?)?\s*\(`?(.+?)`?\))|(?:`?([^\W\d]\w*?)`?(?:\s+((?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)\s*(UNSIGNED\s+)?(?:GENERATED\s+(BY\s+DEFAULT|ALWAYS)\s+AS\s+IDENTITY(?:\s*\((.*?)\))?)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);

                if (!$Columns) {
                    return $this->set_error('Parsing error in CREATE TABLE query');
                }

                $new_columns = array();
                $hasIdentity = false;

                $numColums = count($Columns[0]);
                for ($c = 0; $c < $numColums; ++$c) {
                    if ($Columns[1][$c]) {
                        if (!$Columns[3][$c]) {
                            return $this->set_error("Parse Error: Excepted column name in \"{$Columns[1][$c]}\"");
                        }

                        $keytype = strtolower($Columns[1][$c]);
                        if ($keytype === 'index') {
                            $keytype = 'key';
                        }
                        $keycolumns = explode(',', $Columns[3][$c]);
                        foreach ($keycolumns as $keycolumn) {
                            $new_columns[trim($keycolumn)]['key'] = $keytype[0];
                        }
                    } else {
                        $name = $Columns[4][$c];
                        $type = $Columns[5][$c];
                        $options = $Columns[10][$c];

                        if (isset($new_columns[$name])) {
                            return $this->set_error("Column '{$name}' redefined");
                        }

                        $type = strtoupper($type);
                        if (in_array($type, array('CHAR', 'VARCHAR', 'BINARY', 'VARBINARY', 'TEXT', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT', 'BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB'))) {
                            $type = Types::STRING;
                        } elseif (in_array($type, array('BIT', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'INTEGER', 'BIGINT'))) {
                            $type = Types::INTEGER;
                        } elseif (in_array($type, array('FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION', 'NUMERIC', 'DEC', 'DECIMAL'))) {
                            $type = Types::FLOAT;
                        } else {
                            switch ($type) {
                                case 'DATETIME':
                                    $type = Types::DATETIME;
                                    break;
                                case 'DATE':
                                    $type = Types::DATE;
                                    break;
                                case 'ENUM':
                                    $type = Types::ENUM;
                                    break;
                                case 'TIME':
                                    $type = Types::TIME;
                                    break;
                                default:
                                    return $this->set_error("Column '{$name}' has unknown type '{$type}'");
                            }
                        }

                        if (preg_match("/\bnot\s+null\b/i", $options)) {
                            $null = 0;
                        } else {
                            $null = 1;
                        }

                        $auto = 0;
                        $restraint = null;
                        if (!empty($Columns[8][$c])) {
                            $auto = 1;
                            $always = (int) !strcasecmp($Columns[8][$c], 'ALWAYS');
                            $parsed = $this->parse_sequence_options($Columns[9][$c]);
                            if ($parsed === false) {
                                return false;
                            }

                            $restraint = $this->load_create_sequence($parsed);
                            $start = $restraint[0];
                            array_unshift($restraint, $start, $always);

                            $null = 0;
                        } elseif (preg_match('/\bAUTO_?INCREMENT\b/i', $options)) {
                            $auto = 1;
                            $restraint = array(1, 0, 1, 1, 1, PHP_INT_MAX, 0);
                        }

                        if ($auto) {
                            if ($type !== Types::INTEGER && $type !== Types::FLOAT) {
                                return $this->set_error('Identity columns and autoincrement only allowed on numeric columns');
                            } elseif ($hasIdentity) {
                                return $this->set_error('A table can only have one identity column.');
                            }
                            $hasIdentity = true;
                        }

                        if ($type === Types::ENUM) {
                            preg_match_all("/'(.*?(?<!\\\\))'/", $Columns[6][$c], $values);
                            $restraint = $values[1];
                        }

                        if (preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')/is", $options, $matches)) {
                            if ($auto) {
                                return $this->set_error('Can not specify a default value for an identity column');
                            }

                            $default = $this->parseDefault($matches[1], $type, $null, $restraint);
                        } else {
                            $default = $this->get_type_default_value($type, $null);
                        }

                        if (preg_match('/(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)/is', $options, $keymatches)) {
                            $keytype = strtolower($keymatches[1]);
                            $key = $keytype{0};
                        } else {
                            $key = 'n';
                        }

                        $new_columns[$name] = array('type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint);
                    }
                }

                return new Statements\CreateTable($this, $table_name_pieces, $ifNotExists, $temporary, $new_columns);
            } else {
                $likeClause = isset($matches[4]) ? $matches[4] : '';
                $likeTablePieces = $this->parse_relation_name($matches[3]);
                if ($likeTablePieces === false) {
                    return false;
                }

                $likeOptions = $this->parse_table_like_clause($likeClause);
                if ($likeOptions === false) {
                    return false;
                }

                return new Statements\CreateTableLike($this, $table_name_pieces, $ifNotExists, $temporary, $likeTablePieces, $likeOptions);
            }
        } else {
            return $this->set_error('Invalid CREATE TABLE query');
        }
    }

    public function get_type_default_value($type, $null)
    {
        if ($null) {
            return 'NULL';
        } elseif ($type === Types::STRING) {
            return '';
        } elseif ($type === Types::FLOAT) {
            return 0.0;
        } else {
            return 0;
        }
    }

    private function parse_table_like_clause($likeClause)
    {
        $results = array(CreateTableLike::IDENTITY => false, CreateTableLike::DEFAULTS => false);
        $optionsWords = preg_split('/\s+/', strtoupper($likeClause), -1, PREG_SPLIT_NO_EMPTY);
        $wordCount = count($optionsWords);
        for ($i = 0; $i < $wordCount; ++$i) {
            $firstWord = $optionsWords[$i];
            if ($firstWord === 'INCLUDING') {
                $including = true;
            } elseif ($firstWord === 'EXCLUDING') {
                $including = false;
            } else {
                return $this->set_error('Unexpected token in LIKE clause: '.$firstWord);
            }

            $word = $optionsWords[++$i];
            if ($word === 'IDENTITY') {
                $type = CreateTableLike::IDENTITY;
            } elseif ($word === 'DEFAULTS') {
                $type = CreateTableLike::DEFAULTS;
            } else {
                return $this->set_error('Unknown option after '.$firstWord.': '.$word);
            }

            $results[$type] = $including;
        }

        return $results;
    }

    private function load_create_sequence($parsed)
    {
        $increment = isset($parsed['INCREMENT']) ? (int) $parsed['INCREMENT'] : 1;
        if ($increment === 0) {
            return $this->set_error('Increment of zero in identity column defintion is not allowed');
        }

        $climbing = $increment > 0;
        $min = isset($parsed['MINVALUE']) ? (int) $parsed['MINVALUE'] : ($climbing ? 1 : PHP_INT_MIN);
        $max = isset($parsed['MAXVALUE']) ? (int) $parsed['MAXVALUE'] : ($climbing ? PHP_INT_MAX : -1);
        $cycle = isset($parsed['CYCLE']) ? (int) $parsed['CYCLE'] : 0;

        if (isset($parsed['START'])) {
            $start = (int) $parsed['START'];
            if ($start < $min || $start > $max) {
                return $this->set_error('Identity column start value not inside valid range');
            }
        } elseif ($climbing) {
            $start = $min;
        } else {
            $start = $max;
        }

        return array($start, $increment, $min, $max, $cycle);
    }

    private function parse_sequence_options($options, $isAlter = false)
    {
        $parsed = array();
        if (!empty($options)) {
            if (!$isAlter) {
                $startName = 'START';
            } else {
                $startName = 'RESTART';
            }

            $valueTypes = array($startName, 'INCREMENT', 'MINVALUE', 'MAXVALUE');
            $secondWords = array($startName => 'WITH', 'INCREMENT' => 'BY');
            $startKey = $startName.'WITH';
            $optionsWords = preg_split('/\s+/', strtoupper($options));
            $wordCount = count($optionsWords);
            for ($i = 0; $i < $wordCount; ++$i) {
                $word = $optionsWords[$i];
                if ($isAlter) {
                    if ($word === 'SET') {
                        $word = $optionsWords[++$i];
                        if (!in_array($word, array('INCREMENT', 'CYCLE', 'MAXVALUE', 'MINVALUE', 'GENERATED'))) {
                            return $this->set_error('Unknown option after SET: '.$word);
                        }
                    }

                    if ($word === 'RESTART') {
                        if (($i + 1) == $wordCount || $optionsWords[$i + 1] !== 'WITH') {
                            $parsed['RESTART'] = 'start';
                            continue;
                        }
                    }

                    if ($word === 'GENERATED') {
                        $word = $optionsWords[++$i];
                        if ($word === 'BY') {
                            $word = $optionsWords[++$i];
                            if ($word !== 'DEFAULT') {
                                return $this->set_error('Expected DEFAULT after BY');
                            }
                            $parsed['ALWAYS'] = false;
                        } elseif ($word === 'ALWAYS') {
                            $parsed['ALWAYS'] = true;
                        } else {
                            return $this->set_error('Unexpected word after GENERATED: ' + $word);
                        }
                    }
                }

                if (in_array($word, $valueTypes)) {
                    $original = $word;
                    if (isset($secondWords[$original])) {
                        $word = $optionsWords[++$i];
                        $second = $secondWords[$original];
                        if ($word !== $second) {
                            return $this->set_error('Expected '.$second.' after '.$original);
                        }
                    }

                    $word = $optionsWords[++$i];
                    if (preg_match('/[+-]?\s*\d+(?:\.\d+)?/', $word, $number)) {
                        if (!isset($parsed[$original])) {
                            $parsed[$original] = $number[0];
                        } else {
                            return $this->set_error($original.' already set for this identity/sequence.');
                        }
                    } else {
                        return $this->set_error('Could not parse number after '.$original);
                    }
                } elseif ($word === 'NO') {
                    $word = $optionsWords[++$i];
                    if (in_array($word, array('CYCLE', 'MAXVALUE', 'MINVALUE'))) {
                        if (!isset($parsed[$word])) {
                            $parsed[$word] = null;
                        } else {
                            return $this->set_error($word.' already set for this identity column.');
                        }
                    } else {
                        return $this->set_error('Unknown option after NO: '.$word);
                    }
                } elseif ($word === 'CYCLE') {
                    $parsed[$word] = 1;
                }
            }
        }

        return $parsed;
    }

    private function query_insert($query)
    {
        $this->affected = 0;

        // All INSERT/REPLACE queries are the same until after the table name
        if (preg_match("/\A((INSERT|REPLACE)(?:\s+(IGNORE))?\s+INTO\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?))\s+(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $beginning, $command, $ignore, $full_table_name, $the_rest) = $matches;
        } else {
            return $this->set_error('Invalid Query');
        }

        $table_name_pieces = $this->parse_relation_name($full_table_name);
        if(!$table_name_pieces)
            return false;
        $table = $this->find_table($table_name_pieces);
        if ($table === false) {
            return false;
        } elseif ($table->isReadLocked()) {
            return $this->error_table_read_lock($table_name_pieces);
        }

        $tableColumns = $table->getColumns();
        $tableColumnNames = $table->getColumnNames();

        $check_names = true;
        $isSetVersion = false;
        $replace = !strcasecmp($command, 'REPLACE');

        // Column List present?
        if (preg_match("/^\(((`?[^\W\d]\w*`?\s*,\s*)*`?[^\W\d]\w*`?)\s*\)\s*/is", $the_rest, $matches)) {
            $the_rest = substr($the_rest, strlen($matches[0]));
            $Columns = preg_split("/`?\s*,\s*`?/s", $matches[1]);
        }
        // SET syntax
        elseif (preg_match("/^SET\s+(.+)/is", $the_rest, $matches)) {
            $the_rest = substr($the_rest, strlen($matches[0]));
            $SET = $this->parseSetClause($matches[1], $tableColumns);
            $Columns = [];
            $data_values = [];

            foreach ($SET as $set) {
                $Columns[] = $set[0];
                $data_values[] = $set[1];
            }

            $dataRows = [ $data_values ];
            $isSetVersion = true;
        }
        else {
            $Columns = $tableColumnNames;
            $check_names = false;
        }

        if (preg_match("/^DEFAULT\s+VALUES\s*/is", $the_rest, $matches)) {
            if ($isSetVersion )
                return $this->set_error('Invalid INSERT Query ');
            $defaults = array_fill(0, count($tableColumns), 'DEFAULT');
            $dataRows = [ $defaults ];
        } elseif (preg_match("/^(VALUES\s*\(.+)/is", $the_rest, $matches)) {
            if ($isSetVersion)
                return $this->set_error('Invalid INSERT Query ');
            $dataRows = $this->parseValues($matches[1]);
        } elseif (preg_match("/^SELECT\s+.+/is", $the_rest)) {   // INSERT...SELECT
            if ($isSetVersion)
                return $this->set_error('Invalid INSERT Query ');
            $result = $this->query_select($the_rest);
            while (($values = $result->fetchRow()) !== false) {
                $row = array_map(
                    function ($value) {
                        if (is_string($value)) $value = "'$value'";
                        elseif($value === null) $value = "NULL";
                        return $value;
                    },
                    $values);
                $dataRows[] = $row;
            }
            --$this->query_count;
        } elseif (!$isSetVersion) {
            return $this->set_error('Invalid INSERT Query');
        }

        $Data = [];
        foreach ($dataRows as $dataRow) {
            $NewRow = [];
            if ($check_names) {
                $i = 0;

                if (count($dataRow) != count($Columns)) {
                    return $this->set_error('Number of inserted values and columns not equal');
                }

                foreach ($Columns as $col_name) {
                    if (!in_array($col_name, $tableColumnNames)) {
                        return $this->set_error("Invalid column name '{$col_name}' found");
                    } else {
                        $NewRow[$col_name] = $dataRow[$i++];
                    }
                }
            } else {
                $countData = count($dataRow);
                $countColumns = count($Columns);

                if ($countData < $countColumns) {
                    $NewRow = array_combine($Columns, array_pad($dataRow, $countColumns, 'NULL'));
                } elseif ($countData > $countColumns) {
                    return $this->set_error('Trying to insert too many values');
                } else {
                    $NewRow = array_combine($Columns, $dataRow);
                }
            }

            if (count($Columns) != count($tableColumnNames)) {
                foreach ($tableColumnNames as $col_name) {
                    if (!in_array($col_name, $Columns)) {
                        $NewRow[$col_name] = 'NULL';
                    }
                }
            }
            $Data[] = $NewRow;
        }

        $insert = new Statements\Insert($this, $table_name_pieces, $Data, !empty($ignore), $replace);
        return $insert->execute();
    }

    private function parseValues($query)
    {
        if( !preg_match("/\AVALUES\s*\(\s*/is", $query, $matches))
            return $this->set_error('Invalid VALUES syntax');

        $currentPos = strlen($matches[0]);
        $the_rest = substr($query, $currentPos);
        $stop = false;
        $eos = false;
        $rows = [];

        do
        {
            $row = [];
            while (!$stop && preg_match("/(?:\A|\s*)(DEFAULT|AUTO|NULL|'.*?(?<!\\\\)'|(?:[\+\-]\s*)?\d+(?:\.\d+)?|`?[^\W\d]\w*`?)\s*($|,|\))/Ais", $the_rest, $colmatches)) {
                $eos = empty($colmatches[2]);
                $stop = $eos || $colmatches[2] == ')';
                $row[] = $colmatches[1];
                $currentPos += strlen($colmatches[0]);
                $the_rest = substr($query, $currentPos);
            }

            if ($eos) {
                return $this->set_error('Unexpected end of query in VALUES.');
            }

            $rows[] = $row;
            if( !preg_match("/\A,\s*\(\s*/is", $the_rest, $colmatches)) {
                break;
            }

            $currentPos += strlen($colmatches[0]);
            $the_rest = substr($query, $currentPos);
            $stop = false;

        } while(true);

        return $rows;
    }

    ////Update data in the DB
    private function query_update($query)
    {
        $this->affected = 0;
        if (preg_match("/\AUPDATE(?:\s+(IGNORE))?\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?\s*[;]?\Z/is", $query, $matches)) {
            $set_clause = preg_replace("/(.+?)(\s+WHERE)(.*)/is", '\\1', $matches[3]);
            $ignore = !empty($matches[1]);
            $full_table_name = $matches[2];

            $table_name_pieces = $this->parse_relation_name($full_table_name);
            $table = $this->find_table($table_name_pieces);
            if ($table === false) {
                return false;
            } elseif ($table->isReadLocked()) {
                return $this->error_table_read_lock($table_name_pieces);
            }

            $table_name = $table_name_pieces[2];
            $columns = $table->getColumns();
            $columnNames = array_keys($columns);
            $cursor = $table->getCursor();
            $col_indicies = array_flip($columnNames);

            $SET = $this->parseSetClause($set_clause, $columns);

            $where = null;
            if (isset($matches[4])) {
                $where = $this->build_where($matches[4], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames), self::$WHERE_NORMAL);
                if (!$where) {
                    return $this->set_error('Invalid WHERE clause: '.$this->error_msg);
                }
            }

            $updates = array();
            foreach ($SET as $set) {
                list($column, $value) = $set;
                $new_value = $this->parse_value($columns[$column], $value);
                if ($new_value === false) {
                    return false;
                }
                $col_index = $col_indicies[$column];
                $updates[$col_index] = "$col_index => $new_value";
            }

            $unique_key_columns = array();
            foreach ($columns as $column => $columnDef) {
                if ($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
                    $unique_key_columns[] = $col_indicies[$column];
                }
            }

            $affected = 0;
            $updated_key_columns = array_intersect(array_keys($updates), $unique_key_columns);
            $key_lookup = array();
            if (!empty($updated_key_columns)) {
                foreach ($cursor as $rowId => $entry) {
                    foreach ($updated_key_columns as $unique) {
                        if (!isset($key_lookup[$unique])) {
                            $key_lookup[$unique] = array();
                        }

                        $key_lookup[$unique][$entry[$unique]] = $rowId;
                    }
                }
            }

            $updates = 'array('.implode(',', $updates).')';
            $line = "\t\t\$table->updateRow(\$rowId, \$updates);\r\n";
            $line .= "\t\t\$affected++;\r\n";

            // find all updated columns that are part of a unique key
            // if there are any, call checkUnique to validate still unique.
            $code = '';
            if (!empty($updated_key_columns)) {
                $code = <<<EOV
    \$violation = \$this->where_key_check(\$rowId, \$entry, \$key_lookup, \$updated_key_columns);
    if(\$violation) {
        if(!\$ignore) {
            return \$this->set_error("Duplicate value for unique column '{\$column}'");
        } else {
            continue;
        }
    }

    $line
EOV;
            } else {
                $code = $line;
            }

            if ($where) {
                $code = "\tif({$where}) {\r\n$code\r\n\t}";
            }

            $updateCode = <<<EOC
foreach( \$cursor->rewind(); \$cursor->valid(); \$cursor->next())
{
    \$updates = $updates;
    \$entry = \$updates + \$cursor->current();
$code
}
return true;
EOC;

            $success = eval($updateCode);
            if (!$success) {
                return $success;
            }

            $this->affected = $affected;
            if ($this->affected) {
                if ($this->auto) {
                    $table->commit();
                } elseif (!in_array($table, $this->updatedTables)) {
                    $this->updatedTables[] = $table;
                }
            }

            return true;
        } else {
            return $this->set_error('Invalid UPDATE query');
        }
    }

    private function where_key_check($rowId, $entry, &$key_lookup, $unique_columns)
    {
        foreach ($unique_columns as $unique) {
            $current_lookup = &$key_lookup[$unique];
            $current_val = $entry[$unique];
            if (isset($current_lookup[$current_val])) {
                if ($current_lookup[$current_val] != $rowId) {
                    return true;
                }
            } else {
                $current_lookup[$current_val] = $rowId;
            }
        }

        return false;
    }

    /*
        MERGE INTO
          table_dest d
        USING
          table_source s
        ON
          (s.id = d.id)
        when     matched then update set d.txt = s.txt
        when not matched then insert (id, txt) values (s.id, s.txt);
    */
    private function query_merge($query)
    {
        if (preg_match("/\AMERGE\s+INTO\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s+USING\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s(?:AS\s+)?`?([^\W\d]\w*)`?)?\s+ON\s+(.+?)\s+(WHEN\s+(?:NOT\s+)?MATCHED.+?);?\Z/is", $query, $matches)) {
            list(, $full_dest_table_name, $dest_alias, $full_src_table_name, $src_alias, $on_clause, $matches_clause) = $matches;

            $dest_table_name_pieces = $this->parse_relation_name($full_dest_table_name);
            if (!($dest_table = $this->find_table($dest_table_name_pieces))) {
                return false;
            }

            $src_table_name_pieces = $this->parse_relation_name($full_src_table_name);
            if (!($src_table = $this->find_table($src_table_name_pieces))) {
                return false;
            }

            if (empty($dest_alias)) {
                $dest_alias = $full_dest_table_name[2];
            }

            if (empty($src_alias)) {
                $src_alias = $full_src_table_name[2];
            }

            $src_table_columns = $src_table->getColumns();
            $src_columns_size = count($src_table_columns);
            $joined_info = array(
                'tables' => array($src_alias => $src_table_columns),
                'offsets' => array($src_alias => 0),
                'columns' => array_keys($src_table_columns), );

            $dest_table_columns = $dest_table->getColumns();
            $dest_table_column_names = array_keys($dest_table_columns);
            $dest_column_indices = array_flip($dest_table_column_names);

            $joined_info['tables'][$dest_alias] = $dest_table_columns;
            $new_offset = count($joined_info['columns']);
            $joined_info['columns'] = array_merge($joined_info['columns'], $dest_table_column_names);

            $conditional = $this->build_where($on_clause, $joined_info, self::$WHERE_ON);
            if (!$conditional) {
                return $this->set_error('Invalid ON clause: '.$this->error_msg);
            }

            if (!isset($this->join_lambdas[$conditional])) {
                $join_function = create_function('$left_entry,$right_entry', "return $conditional;");
                $this->join_lambdas[$conditional] = $join_function;
            } else {
                $join_function = $this->join_lambdas[$conditional];
            }

            $joined_info['offsets'][$dest_alias] = $new_offset;

            $hasMatched = false;
            $hasNotMatched = false;
            $matches_clause = trim($matches_clause);
            while (!empty($matches_clause)) {
                if (preg_match("/\AWHEN\s+MATCHED\s+(?:AND\s+(.+?)\s+)?THEN\s+UPDATE\s+SET\s+(.+?)(\s+when\s+.+|\s*\Z)/is", $matches_clause, $clause)) {
                    if ($hasMatched) {
                        return $this->set_error('Can only have one WHEN MATCHED clause');
                    }
                    list(, $andClause, $setList, $matches_clause) = $clause;
                    $sets = $this->parseSetClause($setList, $dest_table_columns, $dest_alias);
                    if ($sets === false) {
                        return false;
                    }

                    $updateCode = '';
                    foreach ($sets as $set) {
                        $valueExpr = $this->build_expression($set[1], $joined_info, self::$WHERE_NORMAL);
                        if ($valueExpr === false) {
                            return false;
                        }
                        $colIndex = $dest_column_indices[$set[0]];
                        $updateCode .= $colIndex." => $valueExpr, ";
                    }
                    $updateCode = 'return array('.substr($updateCode, 0, -2).');';

                    if (!empty($andClause)) {
                        $updateAndExpr = $this->build_where($andClause, $joined_info, self::$WHERE_NORMAL);
                        if (!$updateAndExpr) {
                            return $this->set_error('Invalid AND clause: '.$this->error_msg);
                        }
                        $updateCode = "if($updateAndExpr) { $updateCode } else { return false; }";
                    }
                    $hasMatched = true;
                } elseif (preg_match("/\AWHEN\s+NOT\s+MATCHED\s+(?:AND\s+(.+?)\s+)?THEN\s+INSERT\s*(?:\((.+?)\))?\s*VALUES\s*\((.+?)\)(\s+when\s+.+|\s*\Z)/is", $matches_clause, $clause)) {
                    if ($hasNotMatched) {
                        return $this->set_error('Can only have one WHEN NOT MATCHED clause');
                    }
                    list(, $andClause, $columnList, $values, $matches_clause) = $clause;
                    $columnList = trim($columnList);
                    if (!empty($columnList)) {
                        $columns = preg_split("/\s*,\s*/", columnList);
                    } else {
                        $columns = $dest_table_column_names;
                    }

                    preg_match_all("/\((?:[^()]|(?R))+\)|'[^']*'|[^(),\s]+/", $values, $valuesList);
                    $valuesList = $valuesList[0];

                    if (count($valuesList) != count($columns)) {
                        return $this->set_error('Number of inserted values and columns are not equal in MERGE WHEN NOT MATCHED clause');
                    }

                    $insertCode = '';
                    foreach ($valuesList as $value) {
                        $valueExpr = $this->build_expression($value, $joined_info, self::$WHERE_NORMAL);
                        if ($valueExpr === false) {
                            return false;
                        }
                        $insertCode .= $valueExpr.', ';
                    }
                    $insertCode = 'return array('.substr($insertCode, 0, -2).');';

                    if (!empty($andClause)) {
                        $insertAndExpr = $this->build_where($andClause, $joined_info, self::$WHERE_NORMAL);
                        if (!$insertAndExpr) {
                            return $this->set_error('Invalid AND clause: '.$this->error_msg);
                        }
                        $insertCode = "if($insertAndExpr) { $insertCode } else { return false; }";
                    }

                    $hasNotMatched = true;
                } else {
                    return $this->set_error('Unknown MERGE WHEN clause');
                }

                $matches_clause = trim($matches_clause);
            }

            $joinMatches = array();
            $join_data = $this->left_join($src_table->getEntries(), $dest_table->getEntries(), $join_function, $src_columns_size, $joinMatches);

            $affected = 0;
            $srcCursor = $src_table->getCursor();
            $destCursor = $src_table->getCursor();
            foreach ($srcCursor as $srcRowId => $entry) {
                $destRowId = $joinMatches[$srcRowId];
                if ($destRowId === false) {
                    $newRow = eval($insertCode);
                    if ($newRow !== false) {
                        $dest_table->insertRow($newRow);
                        ++$affected;
                    }
                } else {
                    $destCursor->seek($destRowId);
                    $destRow = $destCursor->getRow();
                    $entry = array_merge($entry, $destRow);
                    $updates = eval($updateCode);
                    if ($updates !== false) {
                        $dest_table->updateRow($destRowId, $updates);
                        ++$affected;
                    }
                }
            }

            if ($this->auto) {
                $dest_table->commit();
            } elseif (!in_array($dest_table, $this->updatedTables)) {
                $this->updatedTables[] = $dest_table;
            }

            $this->affected = $affected;

            return true;
        } else {
            return $this->set_error('Invalid MERGE query');
        }
    }

    ////Select data from the DB
    private function query_select($query)
    {
        if (!preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*)\s*[;]?\s*\Z/is', $query, $matches, PREG_OFFSET_CAPTURE)) {
            return $this->set_error('Invalid SELECT query');
        }

        $distinct = !strncasecmp($matches[1][0], 'DISTINCT', 8);
        $has_random = strlen(trim($matches[2][0])) > 0;
        $isTableless = true;

        $oneAggregate = false;
        $selectedInfo = array();
        $currentPos = $matches[3][1];
        $stop = false;
        while (!$stop && preg_match("/((?:\A|\s*)(?:(-?\d+(?:\.\d+)?)|('.*?(?<!\\\\)')|(?:(`?([^\W\d]\w*)`?\s*\(.*?\)))|(?:(?:(?:`?([^\W\d]\w*)`?\.)?(`?([^\W\d]\w*)`?|\*))))(?:(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?))?\s*)(?:\Z|(from|where|having|(?:group|order)?\s+by|offset|fetch|limit)|,)/Ais", $query, $colmatches, 0, $currentPos)) {
            $stop = !empty($colmatches[10]);
            $idx = !$stop ? 0 : 1;
            $currentPos += strlen($colmatches[$idx]);
            $alias = null;
            if (!empty($colmatches[2])) {  // int/float constant
                $value = $colmatches[2];
                $type = 'number';
                $alias = $value;
            } elseif (!empty($colmatches[3])) {  // string constant
                $value = $colmatches[3];
                $type = 'string';
                $alias = $value;
            } elseif (!empty($colmatches[4])) {  // function call
                $value = $colmatches[4];
                $function_name = strtolower($colmatches[5]);
                $type = 'function';
                list($alias, $function_type) = $this->lookup_function($function_name);
                if ($function_type & Functions::AGGREGATE) {
                    $oneAggregate = true;
                }
            } elseif (!empty($colmatches[7])) {  // column
                $column = $colmatches[7] !== '*' ? $colmatches[8] : $colmatches[7];
                $table_name = $colmatches[6];
                $value = !empty($table_name) ? $table_name.'.'.$column : $column;
                $alias = $column;
                $type = 'column';
            }
            if (!empty($colmatches[9])) {
                $alias = $colmatches[9];
                if (substr($value, -1) == '*') {
                    return $this->set_error("Can't not specify an alias for *");
                }
            }
            $selectedInfo[] = array($type, $value, $alias);
        }

        $data = array();
        $joins = array();
        $joined_info = array('tables' => array(), 'offsets' => array(), 'columns' => array());
        if (preg_match('/\s*from\s+((?:(?!\b(WHERE|HAVING|(?:GROUP|ORDER)\s+BY|OFFSET|FETCH|LIMIT)\b).)+)/Ais', $query, $from_matches, 0, $currentPos)) {
            $isTableless = false;
            $tables = array();

            $currentPos += strlen($from_matches[0]);

            $tbls = explode(',', $from_matches[1]);
            foreach ($tbls as $tbl) {
                if (preg_match('/\A\s*(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s*(.*)/is', $tbl, $table_matches)) {
                    list(, $full_table_name, $saveas, $table_unparsed) = $table_matches;

                    $table_name_pieces = $this->parse_relation_name($full_table_name);
                    if (!($table = $this->find_table($table_name_pieces))) {
                        return false;
                    }

                    if (empty($saveas)) {
                        $saveas = $table_name_pieces[2];
                    }

                    if (!isset($tables[$saveas])) {
                        $tables[$saveas] = $table;
                    } else {
                        return $this->set_error("Table named '$saveas' already specified");
                    }

                    $joins[$saveas] = array('fullName' => $table_name_pieces, 'joined' => array());
                    $table_columns = $table->getColumns();
                    $join_columns_size = count($table_columns);
                    $joined_info['tables'][$saveas] = $table_columns;
                    $joined_info['offsets'][$saveas] = count($joined_info['columns']);
                    $joined_info['columns'] = array_merge($joined_info['columns'], array_keys($table_columns));

                    $join_data = $table->getEntries();

                    if (!empty($table_unparsed)) {
                        preg_match_all("/\s*(?:((?:LEFT|RIGHT|FULL)(?:\s+OUTER)?|INNER)\s+)?JOIN\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s+(USING|ON)\s*(?:(?:\((.*?)\))|(?:(?:\()?((?:\S+)\s*=\s*(?:\S+)(?:\))?)))/is", $table_unparsed, $join);
                        $numJoins = count($join[0]);
                        for ($i = 0; $i < $numJoins; ++$i) {
                            $join_name = trim($join[1][$i]);
                            $join_full_table_name = $join[2][$i];
                            $join_table_saveas = $join[3][$i];

                            $join_table_name_pieces = $this->parse_relation_name($join_full_table_name);
                            if (!($join_table = $this->find_table($join_table_name_pieces))) {
                                return false;
                            }

                            if (empty($join_table_saveas)) {
                                $join_table_saveas = $join_table_name_pieces[2];
                            }

                            if (!isset($tables[$join_table_saveas])) {
                                $tables[$join_table_saveas] = $join_table;
                            } else {
                                return $this->set_error("Table named '$join_table_saveas' already specified");
                            }

                            $clause = $join[4][$i];
                            if (!strcasecmp($clause, 'ON')) {
                                $conditions = isset($join[5][$i]) ? $join[5][$i] : $join[6][$i];
                            } elseif (!strcasecmp($clause, 'USING')) {
                                $shared_columns = preg_split('/\s*,\s*/', trim($join[6][$i]));

                                $conditional = '';
                                foreach ($shared_columns as $shared_column) {
                                    $conditional .= " AND {{left}}.$shared_column=$join_table_alias.$shared_column";
                                }
                                $conditions = substr($conditional, 5);
                            }

                            $join_table_columns = $join_table->getColumns();
                            $join_table_column_names = array_keys($join_table_columns);
                            $joining_columns_size = count($join_table_column_names);

                            $joined_info['tables'][$join_table_saveas] = $join_table_columns;
                            $new_offset = count($joined_info['columns']);
                            $joined_info['columns'] = array_merge($joined_info['columns'], $join_table_column_names);

                            $conditional = $this->build_where($conditions, $joined_info, self::$WHERE_ON);
                            if (!$conditional) {
                                return $this->set_error('Invalid ON/USING clause: '.$this->error_msg);
                            }

                            if (!isset($this->join_lambdas[$conditional])) {
                                $join_function = create_function('$left_entry,$right_entry', "return $conditional;");
                                $this->join_lambdas[$conditional] = $join_function;
                            } else {
                                $join_function = $this->join_lambdas[$conditional];
                            }

                            $joined_info['offsets'][$join_table_saveas] = $new_offset;
                            $joins[$saveas]['joined'][] = array('alias' => $join_table_saveas, 'fullName' => $join_table_name_pieces, 'type' => $join_name, 'clause' => $clause, 'comparator' => $join_function);

                            $joining_entries = $join_table->getEntries();
                            if (!strncasecmp($join_name, 'LEFT', 4)) {
                                $joinMatches = array();
                                $join_data = $this->left_join($join_data, $joining_entries, $join_function, $joining_columns_size, $joinMatches);
                                unset($joinMatches);
                            } elseif (!strncasecmp($join_name, 'RIGHT', 5)) {
                                $join_data = $this->right_join($join_data, $joining_entries, $join_function, $join_columns_size);
                            } elseif (!strncasecmp($join_name, 'FULL', 4)) {
                                $join_data = $this->full_join($join_data, $joining_entries, $join_function, $join_columns_size, $joining_columns_size);
                            } else {
                                $join_data = $this->inner_join($join_data, $joining_entries, $join_function);
                            }

                            $join_columns_size += $joining_columns_size;
                        }
                    }

                    // implicit CROSS JOINs
                    if (!empty($join_data)) {
                        if (!empty($data)) {
                            $new_data = array();
                            foreach ($data as $left_entry) {
                                foreach ($join_data as $right_entry) {
                                    $new_data[] = array_merge($left_entry, $right_entry);
                                }
                            }
                            $data = $new_data;
                        } else {
                            $data = $join_data;
                        }
                    }
                } else {
                    return $this->set_error('Invalid table list');
                }
            }
        }

        $tosort = array();
        $where = null;
        $group_key = null;
        $isGrouping = false;
        $having = null;
        $limit = null;
        $singleRow = false;

        if (preg_match('/\s*WHERE\s+((?:(?!\b(HAVING|(?:GROUP|ORDER)s+BY|OFFSET|FETCH|LIMIT)\b).)+)/Ais', $query, $additional, 0, $currentPos)) {
            $currentPos += strlen($additional[0]);
            $where = $this->build_where($additional[1], $joined_info, self::$WHERE_NORMAL);
            if (!$where) {
                return $this->set_error('Invalid WHERE clause: '.$this->error_msg);
            }
        }

        if (preg_match('/\s*GROUP\s+BY\s+((?:(?!\b(HAVING|ORDER\s+BY|OFFSET|FETCH|LIMIT)\b).)+)/Ais', $query, $additional, 0, $currentPos)) {
            $currentPos += strlen($additional[0]);
            $GROUPBY = explode(',', $additional[1]);
            $joined_info['group_columns'] = array();
            $isGrouping = true;
            $group_array = array();
            $group_key_list = '';
            foreach ($GROUPBY as $group_item) {
                if (preg_match('/(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?/is', $group_item, $additional)) {
                    list(, $table_alias, $column) = $additional;
                    $group_col = $this->find_column($column, $table_alias, $joined_info, 'GROUP BY clause');
                    if ($group_col === false) {
                        return false;
                    }

                    $group_array[] = $group_col;
                    $group_key_list .= '$entry['.$group_col.'], ';
                    $joined_info['group_columns'][] = $group_col;
                }
            }

            $group_key = substr($group_key_list, 0, -2);
            if (count($group_array) > 1) {
                $group_key = 'serialize(array('.$group_key.'))';
            }
        }

        if (preg_match('/\s*HAVING\s+((?:(?!\b(ORDER\s+BY|OFFSET|FETCH|LIMIT)\b).)+)/Ais', $query, $additional, 0, $currentPos)) {
            $currentPos += strlen($additional[0]);
            if (!isset($joined_info['group_columns'])) { // no GROUP BY
                $joined_info['group_columns'] = array();
                $isGrouping = true;
                $singleRow = true;
            }

            $having = $this->build_where($additional[1], $joined_info, self::$WHERE_HAVING);
            if (!$having) {
                return $this->set_error('Invalid HAVING clause: '.$this->error_msg);
            }
        }

        if (preg_match('/\s*ORDER\s+BY\s+((?:(?!\b(OFFSET|FETCH|LIMIT)\b).)+)/Ais', $query, $additional, 0, $currentPos)) {
            $currentPos += strlen($additional[0]);
            $ORDERBY = explode(',', $additional[1]);
            foreach ($ORDERBY as $order_item) {
                if (preg_match('/(?:(?:(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?)|(\d+))(?:\s+(ASC|DESC))?(?:\s+NULLS\s+(FIRST|LAST))?/is', $order_item, $additional)) {
                    if (!empty($additional[2])) {
                        // table column name
                        list(, $table_alias, $column) = $additional;
                        $index = $this->find_column($column, $table_alias, $joined_info, 'ORDER BY clause');
                        if ($index === false) {
                            return false;
                        }
                        $key = array($table_alias, $column);
                    } else {
                        // table column number (starts at 1).
                        $number = $additional[3];
                        $key = $number - 1;
                    }

                    $ascend = !empty($additional[4]) ? !strcasecmp('ASC', $additional[4]) : true;
                    $nulls_first = !empty($additional[5]) ? !strcasecmp('FIRST', $additional[5]) : true;
                    $tosort[] = array('key' => $key, 'ascend' => $ascend, 'nullsFirst' => $nulls_first);
                }
            }
        }

        $limit_start = null;
        if (preg_match('/\s*OFFSET\s+(\d+)\s+ROWS?\b/Ais', $query, $additional, 0, $currentPos)) {
            $currentPos += strlen($additional[0]);
            $limit_start = (int) $additional[1];
        }

        if (preg_match('/\s*FETCH\s+(?:FIRST|NEXT)\s+(?:(\d+)\s+)?ROWS?\s+ONLY\b/Ais', $query, $additional, 0, $currentPos)) {
            $currentPos += strlen($additional[0]);
            $limit_stop = isset($additional[1]) ? (int) $additional[1] : 1;
            if ($limit_start === null) {
                $limit_start = 0;
            }
            $limit = array($limit_start, $limit_stop);
        } elseif ($limit_start !== null) {
            // OFFSET without FETCH FIRST
            $limit = array($limit_start, null);
        }

        if (preg_match('/\s*LIMIT\s+(?:(?:(\d+)\s*,\s*(\-1|\d+))|(?:(\d+)\s+OFFSET\s+(\d+))|(\d+))/Ais', $query, $additional, 0, $currentPos)) {
            if ($limit === null) {
                if (isset($additional[5])) {
                    // LIMIT length
                    $limit_stop = $additional[5];
                    $limit_start = 0;
                } elseif (isset($additional[3])) {
                    // LIMIT length OFFSET offset (mySQL, Postgres, SQLite)
                    $limit_stop = $additional[3];
                    $limit_start = $additional[4];
                } else {
                    // LIMIT offset, length (mySQL, SQLite)
                    list(, $limit_start, $limit_stop) = $additional;
                }

                $limit = array((int) $limit_start, (int) $limit_stop);
            } else {
                return $this->set_error('LIMIT forbidden when FETCH FIRST or OFFSET already specified');
            }
        }

        if (!$isGrouping && $oneAggregate) {
            $isGrouping = true;
            $singleRow = true;
        }

        $selected_columns = array();
        $final_code = null;
        if ($isGrouping) {
            $select_line = '';
            foreach ($selectedInfo as $info) {
                list($select_type, $select_value, $select_alias) = $info;
                $column_info = null;
                switch ($select_type) {
                    case 'column':
                        if (strpos($select_value, '.') !== false) {
                            list($table_name, $column) = explode('.', $select_value);
                        } else {
                            $table_name = null;
                            $column = $select_value;
                        }

                        if (!strcasecmp($select_value, 'NULL')) {
                            $select_line .= 'NULL, ';
                            $selected_columns[] = $select_alias;
                            continue;
                        } else {
                            $index = $this->find_column($column, $table_name, $joined_info, 'SELECT clause');
                            if ($index === false) {
                                return false;
                            }
                        }

                        if (!in_array($index, $group_array)) {
                            return $this->set_error("Selected column '{$joined_info['columns'][$index]}' is not a grouped column");
                        }
                        $select_line .= "\$group[0][$index], ";
                        $selected_columns[] = $select_alias;
                        break;
                    case 'number':
                    case 'string':
                        $select_line .= $select_value.', ';
                        $selected_columns[] = $select_alias;
                        break;
                    case 'function':
                        $expr = $this->build_expression($select_value, $joined_info, self::$WHERE_NORMAL);
                        if ($expr === false) {
                            return false;
                        }
                        $select_line .= $expr.', ';
                        $selected_columns[] = $select_alias;
                        break;
                }
                $column_info['name'] = $select_alias;
                $fullColumnsInfo[] = $column_info;
            }

            if (!$singleRow) {
                $line = '$grouped_set['.$group_key.'][] = $entry;';
            } else {
                $line = '$group[] = $entry;';
            }

            $final_line = '$final_set[] = array('.substr($select_line, 0, -2).');';
            $grouped_set = array();

            if ($having !== null) {
                $final_line = "if({$having}) {\r\n\t\t\t\t\t\t$final_line\r\n\t\t\t\t\t}";
            }

            if (!$singleRow) {
                $final_code = <<<EOT
                foreach(\$grouped_set as \$group) {
                    $final_line
                }
EOT;
            } else {
                $final_code = $final_line;
            }
        } else {
            $select_line = '';
            foreach ($selectedInfo as $info) {
                list($select_type, $select_value, $select_alias) = $info;
                switch ($select_type) {
                // function call
                case 'function':
                    $expr = $this->build_expression($select_value, $joined_info, false);
                    if ($expr !== false) {
                        $select_line .= $expr.', ';
                        $selected_columns[] = $select_alias;
                    } else {
                        return false; // error should already be set by parser
                    }
                    break;

                case 'column':
                    if (strpos($select_value, '.') !== false) {
                        list($table_name, $column) = explode('.', $select_value);
                    } else {
                        $table_name = null;
                        $column = $select_value;
                    }

                    if ($column === '*') {
                        $star_tables = !empty($table_name) ? array($table_name) : array_keys($tables);
                        foreach ($star_tables as $tname) {
                            $start_index = $joined_info['offsets'][$tname];
                            $table_columns = $joined_info['tables'][$tname];
                            $column_names = array_keys($table_columns);
                            foreach ($column_names as $index => $column_name) {
                                $select_value = $start_index + $index;
                                $select_line .= "\$entry[$select_value], ";
                                $selected_columns[] = $column_name;
                            }
                        }

                        continue;
                    } elseif (!strcasecmp($select_value, 'NULL')) {
                        $select_line .= 'NULL, ';
                        $selected_columns[] = $select_alias;
                        continue;
                    } else {
                        $index = $this->find_column($column, $table_name, $joined_info, 'SELECT clause');
                        if ($index === false) {
                            return false;
                        }
                        $select_line .= "\$entry[$index], ";
                        $selected_columns[] = $select_alias;
                    }
                    break;

                case 'number':
                case 'string':
                    $select_line .= "$select_value, ";
                    $selected_columns[] = $select_alias;
                    break;

                default:
                    return $this->set_error("Parse Error: Unknown value in SELECT clause: $column");
                }
            }

            $line = '$final_set[] = array('.substr($select_line, 0, -2).');';
            $group = $data;
        }

        if (!empty($joins)) {
            if ($where !== null) {
                $line = "if({$where}) {\r\n\t\t\t\t\t$line\r\n\t\t\t\t}";
            }

            $code = <<<EOT
            foreach(\$data as \$entry) {
                $line
            }
$final_code
EOT;
        } else { // Tableless SELECT
            $entry = array(true);  // hack so it passes count and !empty expressions
            $code = $line;
        }

        $final_set = array();
        eval($code);

        if (!empty($tosort)) {
            foreach ($tosort as &$sort) {
                $key = $sort['key'];

                if (is_int($key)) {
                    if (!isset($selected_columns[$key])) {
                        return $this->set_error('ORDER BY: Invalid column number: '.($key + 1));
                    }
                } else {
                    list($table_name, $column_name) = $key;
                    $index = array_search($column_name, $selected_columns);
                    if ($index === false) {
                        return $this->set_error('ORDER BY: column/alias not in the SELECT list: '.$column_name);
                    }
                    $sort['key'] = $index;
                }
            }

            $orderBy = new OrderByClause($tosort);
            $orderBy->sort($final_set);
        }

        if ($limit !== null) {
            $stop = $limit[1];
            if ($stop !== null) {
                $final_set = array_slice($final_set, $limit[0], $stop);
            } else {
                $final_set = array_slice($final_set, $limit[0]);
            }
        }

        if (!empty($final_set) && $has_random && preg_match("/\s+RANDOM(?:\((\d+)\)?)?\s+/is", $select, $additional)) {
            $results = array();
            if (!$additional[1]) {
                $additional[1] = 1;
            }
            if ($additional[1] <= count($this_random)) {
                $random = array_rand($final_set, $additional[1]);
                if (is_array($random)) {
                    foreach ($random as $key) {
                        $results[] = $final_set[$key];
                    }
                } else {
                    $results[] = $final_set[$random];
                }
            }
            unset($final_set);
            $final_set = $results;
        }

        return new ResultSet($selected_columns, $final_set);
    }

    private function find_column($column, $table_name, $joined_info, $where)
    {
        if (!empty($table_name)) {
            if (!isset($joined_info['tables'][$table_name])) {
                return $this->set_error("Unknown table name/alias in $where: $table_name");
            }

            $index = array_search($column, array_keys($joined_info['tables'][$table_name])) + $joined_info['offsets'][$table_name];
            if ($index === false || $index === null) {
                return $this->set_error("Unknown column in $where: $column");
            }
        } else {
            $index = $this->find_exactly_one($joined_info, $column, $where);
            if ($index === false) {
                return false;
            }
        }

        return $index;
    }

    private function parseSetClause($clause, $columns, $tableAlias = null)
    {
        $result = array();
        if (preg_match_all("/((?:\S+)\s*=\s*(?:'(?:.*?)'|\S+))`?\s*(?:,|\Z)/is", $clause, $sets)) {
            foreach ($sets[1] as $set) {
                $s = preg_split("/`?\s*=\s*`?/", $set);
                if (preg_match("/\A\s*(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?/is", $s[0], $namePieces)) {
                    list(, $prefix, $columnName) = $namePieces;
                    if ($tableAlias !== null && !empty($prefix) && $tableAlias !== $prefix) {
                        return $this->set_error('Unknown table alias in SET clause');
                    }
                    $s[0] = $columnName;
                }

                $result[] = $s;
                if (!isset($columns[$s[0]])) {
                    return $this->set_error("Invalid column name '{$s[0]}' found in SET clause");
                }
            }
        } else {
            $result[0] = preg_split("/\s*=\s*/", $clause);
        }

        return $result;
    }

    private function build_expression($exprStr, $join_info, $where_type)
    {
        $expr = false;

        // function call
        if (preg_match('/\A([^\W\d]\w*)\s*\((.*?)\)/is', $exprStr, $matches)) {
            $function = strtolower($matches[1]);
            $params = $matches[2];
            $final_param_list = '';
            $paramExprs = array();
            $expr_type = '"non-constant"';

            $functionInfo = $this->lookup_function($function);
            if ($functionInfo === false) {
                return false;
            }

            list($function, $type) = $functionInfo;
            $isAggregate = $type & Functions::AGGREGATE;

            if ($isAggregate) {
                $paramExprs[] = '$group';
            }

            if ($type & Functions::CUSTOM_PARSE) {
                $originalFunction = $function;
                $parseFunction = "parse_{$function}_function";
                $parsedData = $this->$parseFunction($join_info, $where_type | 1, $params);
                if ($parsedData !== false) {
                    list($function, $paramExprs) = $parsedData;
                    if (isset($parsedData[2])) {  // used by CAST() to override based on params
                        $columnData['type'] = $parsedData[2];
                    }
                } else {
                    return $this->set_error("Error parsing parameters for function $originalFunction");
                }
            } elseif (strlen($params) !== 0) {
                $parameter = explode(',', $params);
                foreach ($parameter as $param) {
                    $param = trim($param);

                    if ($isAggregate && $param === '*') {
                        if ($function === 'count') {
                            $paramExprs[] = '"*"';
                        } else {
                            return $this->set_error('Passing * as a paramter is only allowed in COUNT');
                        }
                    } else {
                        $paramExpr = $this->build_expression($param, $join_info, $where_type | 1);
                        if ($paramExpr === false) { // parse error
                            return false;
                        }

                        if ($isAggregate) {
                            if (preg_match('/\\$entry\[(\d+)\]/', $paramExpr, $paramExpr_matches)) {
                                $paramExprs[] = $paramExpr_matches[1];
                            } else {
                                //assume everything else is some form of constant

                                $expr_type = '"constant"';
                                $paramExprs[] = $pexp;
                            }
                        } else {
                            $paramExprs[] = $paramExpr;
                        }
                    }
                }
            }

            if ($isAggregate) {
                $paramExprs[] = $expr_type;
            }

            $final_param_list = implode(',', $paramExprs);

            if ($type != Functions::REGISTERED) {
                $expr = "\$this->functions->$function($final_param_list)";
            } else {
                $expr = "$function($final_param_list)";
            }
        }
        // column/alias/keyword
        elseif (preg_match('/\A(?:`?([^\W\d]\w*|\{\{left\}\})`?\.)?`?([^\W\d]\w*)`?\Z/is', $exprStr, $matches)) {
            list(, $table_name, $column) = $matches;
            // table.column
            if ($table_name) {
                if (isset($join_info['tables'][$table_name])) {
                    $table_columns = $join_info['tables'][$table_name];
                    if (isset($table_columns[$column])) {
                        if (isset($join_info['offsets'][$table_name])) {
                            $colIndex = array_search($column, array_keys($table_columns)) + $join_info['offsets'][$table_name];
                            if ($where_type === self::$WHERE_HAVING) { // column/alias in grouping clause
                                if (in_array($colIndex, $join_info['group_columns'])) {
                                    $expr = "\$group[0][$colIndex]";
                                } else {
                                    return $this->set_error("Column $column is not a grouped column");
                                }
                            } else {
                                $expr = ($where_type & self::$WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
                            }
                        } else {
                            $colIndex = array_search($column, array_keys($table_columns));
                            $expr = "\$right_entry[$colIndex]";
                        }
                    } else {
                        return $this->set_error("Unknown column $column for table $table_name");
                    }
                } elseif ($where_type & self::$WHERE_ON && $table_name === '{{left}}') {
                    $colIndex = $this->find_exactly_one($joined_info, $column, 'expression');
                    if ($colIndex === false) {
                        return false;
                    }
                    $expr = "\$left_entry[$colIndex]";
                } else {
                    return $this->set_error('Unknown table/alias '.$table_name);
                }
            }
            // null/unkown
            elseif (!strcasecmp($exprStr, 'NULL') || !strcasecmp($exprStr, 'UNKNOWN')) {
                $expr = 'NULL';
            }
            // true/false
            elseif (!strcasecmp($exprStr, 'TRUE') || !strcasecmp($exprStr, 'FALSE')) {
                $expr = strtoupper($exprStr);
            } else {  // column/alias no table
                $colIndex = $this->find_exactly_one($join_info, $column, 'expression');
                if ($colIndex === false) {
                    return false;
                }
                if ($where_type === self::$WHERE_HAVING) { // column/alias in grouping clause
                    if (in_array($colIndex, $join_info['group_columns'])) {
                        $expr = "\$group[0][$colIndex]";
                    } else {
                        return $this->set_error("Column $column is not a grouped column");
                    }
                } else {
                    $expr = ($where_type & self::$WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
                }
            }
        }
        // number
        elseif (preg_match('/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is', $exprStr)) {
            $expr = $exprStr;
        }
        // string
        elseif (preg_match("/\A'.*?(?<!\\\\)'\Z/is", $exprStr)) {
            $expr = $exprStr;
        } elseif (($where_type & self::$WHERE_ON) && preg_match('/\A{{left}}\.`?([^\W\d]\w*)`?/is', $exprStr, $matches)) {
            $colIndex = $this->find_exactly_one($join_info, $column, 'expression');
            if ($colIndex === false) {
                return false;
            }

            $expr = "\$left_entry[$colIndex]";
        } else {
            return false;
        }

        return $expr;
    }

    private function find_exactly_one($join_info, $column, $location)
    {
        $keys = array_keys($join_info['columns'], $column);
        $keyCount = count($keys);
        if ($keyCount == 0) {
            return $this->set_error("Unknown column/alias in $location: $column");
        } elseif ($keyCount > 1) {
            return $this->set_error("Ambiguous column/alias in $location: $column");
        }

        return $keys[0];
    }

    private function build_where($statement, $join_info, $where_type)
    {
        if ($statement) {
            preg_match_all("/(\A\s*|\s+(?:AND|OR)\s+)(NOT\s+)?(\S+?)(\s*(?:!=|<>|>=|<=>?|>|<|=)\s*|\s+(?:IS(?:\s+NOT)?|(?:NOT\s+)?IN|(?:NOT\s+)?R?LIKE|(?:NOT\s+)?REGEXP)\s+)(\((.*?)\)|'.*?'|\S+)/is", $statement, $WHERE, PREG_SET_ORDER);

            if (empty($WHERE)) {
                return false;
            }

            $condition = '';
            foreach ($WHERE as $where) {
                $local_condition = '';
                $logicalOp = trim($where[1]);
                $not = !empty($where[2]);
                $leftStr = $where[3];
                $operator = preg_replace('/\s+/', ' ', trim(strtoupper($where[4])));
                $rightStr = $where[5];

                $leftExpr = $this->build_expression($leftStr, $join_info, $where_type);
                if ($leftExpr === false) {
                    return false;
                }

                if ($operator !== 'IN' && $operator !== 'NOT IN') {
                    $rightExpr = $this->build_expression($rightStr, $join_info, $where_type);
                    if ($rightExpr === false) {
                        return false;
                    }

                    switch ($operator) {
                        case '=':
                            $local_condition = "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case '!=':
                        case '<>':
                            $local_condition = "(($leftExpr != $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case '>':
                            $local_condition = "(($leftExpr > $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case '>=':
                            $local_condition = "(($leftExpr >= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case '<':
                            $local_condition = "(($leftExpr < $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case '<=':
                            $local_condition = "(($leftExpr <= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case '<=>':
                            $local_condition = "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            break;
                        case 'IS NOT':
                            $not = !$not;
                        case 'IS':
                            if ($rightExpr === 'NULL') {
                                $local_condition = "($leftExpr === null ? FSQL_TRUE : FSQL_FALSE)";
                            } elseif ($rightExpr === 'TRUE') {
                                $local_condition = "\$this->functions->isTrue($leftExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            } elseif ($rightExpr === 'FALSE') {
                                $local_condition = "\$this->functions->isFalse($leftExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            } else {
                                return false;
                            }
                            break;
                        case 'NOT LIKE':
                            $not = !$not;
                        case 'LIKE':
                            $local_condition = "\$this->functions->like($leftExpr, $rightExpr)";
                            break;
                        case 'NOT RLIKE':
                        case 'NOT REGEXP':
                            $not = !$not;
                        case 'RLIKE':
                        case 'REGEXP':
                            $local_condition = "\$this->functions->regexp($leftExpr, $rightExpr)";
                            break;
                        default:
                            $local_condition = "$leftExpr $operator $rightExpr";
                            break;
                    }
                } else {
                    if (!empty($where[6])) {
                        $array_values = explode(',', $where[6]);
                        $valuesExpressions = array();
                        foreach ($array_values as $value) {
                            $valueExpr = $this->build_expression(trim($value), $join_info, $where_type);
                            $valuesExpressions[] = $valueExpr['expression'];
                        }
                        $valuesString = implode(',', $valuesExpressions);
                        $local_condition = "\$this->functions->in($leftExpr, array($valuesString))";

                        if ($operator === 'NOT IN') {
                            $not = !$not;
                        }
                    } else {
                        return false;
                    }
                }

                if (!strcasecmp($logicalOp, 'AND')) {
                    $condition .= ' & ';
                } elseif (!strcasecmp($logicalOp, 'OR')) {
                    $condition .= ' | ';
                }

                if ($not) {
                    $condition .= '\$this->functions->not('.$local_condition.')';
                } else {
                    $condition .= $local_condition;
                }
            }

            return "($condition) === ".FSQL_TRUE;
        }

        return false;
    }

    ////Delete data from the DB
    private function query_delete($query)
    {
        $this->affected = 0;
        if (preg_match('/\ADELETE\s+FROM\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+WHERE\s+(.+?))?\s*[;]?\Z/is', $query, $matches)) {
            $table_name_pieces = $this->parse_relation_name($matches[1]);
            $table = $this->find_table($table_name_pieces);
            if ($table === false) {
                return false;
            } elseif ($table->isReadLocked()) {
                return $this->error_table_read_lock($table_name_pieces);
            }

            $columns = $table->getColumns();
            $columnNames = array_keys($columns);
            $cursor = $table->getCursor();

            if ($cursor->isDone()) {
                return true;
            }

            if (isset($matches[2])) {
                $where = $this->build_where($matches[2], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames), self::$WHERE_NORMAL);
                if (!$where) {
                    return $this->set_error('Invalid WHERE clause: '.$this->error_msg);
                }

                $code = <<<EOC
            for(\$cursor->rewind(); \$cursor->valid(); \$cursor->next()) {
                \$entry = \$cursor->current();
                if({$where})
                {
                    \$table->deleteRow(\$cursor->key());
                    \$this->affected++;
                }
            }
EOC;

                eval($code);
            } else {
                $c = 0;
                for ($cursor->rewind(); $cursor->valid(); $cursor->next()) {
                    $table->deleteRow($cursor->key());
                    ++$c;
                }
                $this->affected = $c;
            }

            if ($this->affected) {
                if ($this->auto) {
                    $table->commit();
                } elseif (!in_array($table, $this->updatedTables)) {
                    $this->updatedTables[] = $table;
                }
            }

            return true;
        } else {
            return $this->set_error('Invalid DELETE query');
        }
    }

    private function query_alter($query)
    {
        if (preg_match("/\AALTER\s+(TABLE|SEQUENCE)\s+(?:(IF\s+EXISTS)\s+)?(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $type, $ifExists, $definition) = $matches;
            $ifExists = !empty($ifExists);
            if (!strcasecmp($type, 'TABLE')) {
                return $this->query_alter_table($definition, $ifExists);
            } else {
                return $this->query_alter_sequence($definition, $ifExists);
            }
        } else {
            return $this->set_error('Invalid ALTER query');
        }
    }

    private function query_alter_sequence($definition, $ifExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.+?)\s*[;]?\Z/is", $definition, $matches)) {
            list(, $fullSequenceName, $valuesList) = $matches;

            $seqNamePieces = $this->parse_relation_name($fullSequenceName);

            $schema = $this->find_schema($seqNamePieces[0], $seqNamePieces[1]);
            if ($schema === false) {
                return false;
            }

            $sequenceName = $seqNamePieces[2];
            $sequence = $schema->getRelation($sequenceName);
            if ($sequence === false) {
                if (!$ifExists) {
                    return $this->error_relation_not_exists($seqNamePieces, 'Sequence');
                } else {
                    return true;
                }
            }

            if (!($sequence instanceof Sequence)) {
                $fullName = $this->build_relation_name($seqNamePieces);

                return $this->set_error("Relation {$fullName} is not a sequence");
            }

            $parsed = $this->parse_sequence_options($valuesList, true);
            if ($parsed === false) {
                return false;
            }

            $sequences = $schema->getSequences();
            if (!$sequences->exists()) {
                $sequences->create();
            }

            $result = $sequence->alter($parsed);
            if ($result !== true) {
                $sequence->load();  // refresh temp changes made
                return $this->set_error($result);
            }

            return true;
        } else {
            return $this->set_error('Invalid ALTER SEQUENCE query');
        }
    }

    private function query_alter_table($definition, $ifExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.*)/is", $definition, $matches)) {
            list(, $fullTableName, $changes) = $matches;

            $tableNamePieces = $this->parse_relation_name($fullTableName);
            $tableObj = $this->find_table($tableNamePieces);
            if ($tableObj === false) {
                if (!$ifExists) {
                    return $this->error_table_not_exists($tableNamePieces);
                } else {
                    return true;
                }
            } elseif ($tableObj->isReadLocked()) {
                return $this->error_table_read_lock($tableNamePieces);
            }

            $tableName = $tableNamePieces[2];
            $columns = $tableObj->getColumns();

            preg_match_all("/(?:ADD|ALTER|DROP|RENAME).*?(?:,|\Z)/is", trim($changes), $specs);
            $specCount = count($specs[0]);
            for ($i = 0; $i < $specCount; ++$i) {
                if (preg_match("/\AADD\s+(?:CONSTRAINT\s+`?[^\W\d]\w*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/is", $specs[0][$i], $matches)) {
                    $columnDef = &$columns[$matches[1]];

                    foreach ($columns as $name => $column) {
                        if ($column['key'] == 'p') {
                            return $this->set_error('Primary key already exists');
                        }
                    }

                    $columnDef['key'] = 'p';
                    $tableObj->setColumns($columns);

                    return true;
                } elseif (preg_match("/\AALTER(?:\s+(?:COLUMN))?\s+`?([^\W\d]\w*)`?\s+(.+?)(?:,|;|\Z)/is", $specs[0][$i], $matches)) {
                    list(, $columnName, $the_rest) = $matches;
                    if (!isset($columns[$columnName])) {
                        return $this->set_error("Column named '$columnName' does not exist in table '$tableName'");
                    }

                    $columnDef = &$columns[$columnName];
                    if (preg_match("/(?:SET\s+DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')|DROP\s+DEFAULT)/is", $the_rest, $defaults)) {
                        if (!empty($defaults[1])) {
                            $default = $this->parseDefault($defaults[1], $columnDef['type'], $columnDef['null'], $columnDef['restraint']);
                        } else {
                            $default = $this->get_type_default_value($columnDef['type'], $columnDef['null']);
                        }

                        $columnDef['default'] = $default;
                        $tableObj->setColumns($columns);
                    } elseif (preg_match("/\ADROP\s+IDENTITY/i", $the_rest, $defaults)) {
                        if (!$columnDef['auto']) {
                            return $this->set_error("Column $columnName is not an identity column");
                        }
                        $tableObj->dropIdentity();
                    } else {
                        $parsed = $this->parse_sequence_options($the_rest, true);
                        if ($parsed === false) {
                            return false;
                        } elseif (!empty($parsed)) {
                            if (!$columnDef['auto']) {
                                return $this->set_error("Column $columnName is not an identity column");
                            }

                            $identity = $tableObj->getIdentity();
                            $result = $identity->alter($parsed);
                            if ($result !== true) {
                                $identity->load();  // refresh temp changes made
                                return $this->set_error($result);
                            }
                        }
                    }

                    return true;
                } elseif (preg_match("/\ADROP\s+PRIMARY\s+KEY/is", $specs[0][$i], $matches)) {
                    $found = false;
                    foreach ($columns as $name => $column) {
                        if ($column['key'] == 'p') {
                            $columns[$name]['key'] = 'n';
                            $found = true;
                        }
                    }

                    if ($found) {
                        $tableObj->setColumns($columns);

                        return true;
                    } else {
                        return $this->set_error('No primary key found');
                    }
                } elseif (preg_match("/\ARENAME\s+(?:TO\s+)?(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is", $specs[0][$i], $matches)) {
                    $newTableNamePieces = $this->parse_relation_name($matches[1]);
                    if ($newTableNamePieces === false) {
                        return false;
                    }

                    $schema = $tableObj->schema();

                    $newSchema = $this->find_schema($newTableNamePieces[0], $newTableNamePieces[1]);
                    if ($newSchema === false) {
                        return false;
                    }

                    $newTable = $newSchema->getTable($newTableNamePieces[2]);
                    if ($newTable->exists()) {
                        return $this->set_error("Destination table {$newTable->fullName()} already exists");
                    }

                    return $schema->renameTable($tableName, $newTable->name(), $newSchema);
                } else {
                    return $this->set_error('Invalid ALTER TABLE query');
                }
            }
        } else {
            return $this->set_error('Invalid ALTER TABLE query');
        }
    }

    private function parseDefault($default, $type, $null, $restraint)
    {
        if (strcasecmp($default, 'NULL')) {
            if (preg_match("/\A'(.*)'\Z/is", $default, $matches)) {
                if ($type == Types::INTEGER) {
                    $default = (int) $matches[1];
                } elseif ($type == Types::FLOAT) {
                    $default = (float) $matches[1];
                } elseif ($type == Types::ENUM) {
                    $default = $matches[1];
                    if (in_array($default, $restraint)) {
                        $default = array_search($default, $restraint) + 1;
                    } else {
                        $default = 0;
                    }
                } elseif ($type == Types::STRING) {
                    $default = $matches[1];
                }
            } else {
                if ($type == Types::INTEGER) {
                    $default = (int) $default;
                } elseif ($type == Types::FLOAT) {
                    $default = (float) $default;
                } elseif ($type == Types::ENUM) {
                    $default = (int) $default;
                    if ($default < 0 || $default > count($restraint)) {
                        return $this->set_error('Numeric ENUM value out of bounds');
                    }
                } elseif ($type == Types::STRING) {
                    $default = "'".$matches[1]."'";
                }
            }
        } elseif (!$null) {
            $default = $this->get_type_default_value($type, 0);
        }

        return $default;
    }

    private function query_rename($query)
    {
        if (preg_match("/\ARENAME\s+TABLE\s+(.*)\s*[;]?\Z/is", $query, $matches)) {
            $tables = explode(',', $matches[1]);
            foreach ($tables as $table) {
                list($old, $new) = preg_split("/\s+TO\s+/i", trim($table));

                if (preg_match("/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is", $old, $table_parts)) {
                    $old_table_pieces = $this->parse_relation_name($table_parts[1]);
                    if ($old_table_pieces === false) {
                        return false;
                    }

                    $old_schema = $this->find_schema($old_table_pieces[0], $old_table_pieces[1]);
                    if ($old_schema === false) {
                        return false;
                    }
                } else {
                    return $this->set_error('Parse error in table listing');
                }

                if (preg_match("/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is", $new, $table_parts)) {
                    $new_table_pieces = $this->parse_relation_name($table_parts[1]);
                    if ($new_table_pieces === false) {
                        return false;
                    }

                    $new_schema = $this->find_schema($new_table_pieces[0], $new_table_pieces[1]);
                    if ($new_schema === false) {
                        return false;
                    }
                } else {
                    return $this->set_error('Parse error in table listing');
                }

                $old_table_name = $old_table_pieces[2];
                $old_table = $old_schema->getTable($old_table_name);
                if ($old_table->exists()) {
                    return false;
                } elseif ($old_table->isReadLocked()) {
                    return $this->error_table_read_lock($old_table_pieces);
                }

                $new_table_name = $new_table_pieces[2];
                $new_table = $new_schema->getTable($new_table_name);
                if ($new_table->exists()) {
                    return $this->set_error("Destination table {$new_table->fullName()} already exists");
                }

                return $old_schema->renameTable($old_table_name, $new_table_name, $new_schema);
            }

            return true;
        } else {
            return $this->set_error('Invalid RENAME query');
        }
    }

    private function query_drop($query)
    {
        if (preg_match("/\ADROP\s+((?:TEMPORARY\s+)?TABLE|(?:S(?:CHEMA|EQUENCE)|DATABASE))\s+(?:(IF\s+EXISTS)\s+)?(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $type, $ifExists, $namesList) = $matches;
            $type = strtolower($type);
            $ifExists = !empty($ifExists);
            if (substr($type, -5) === 'TABLE') {
                $type = 'table';
            }
            $dropFunction = array($this, 'query_drop_'.$type);
            $names = preg_split('/\s*,\s*/', $namesList);

            foreach ($names as $name) {
                $result = $this->$dropFunction($name, $ifExists);
                if ($result === false) {
                    return false;
                }
            }

            return true;
        } else {
            return $this->set_error('Invalid DROP query');
        }
    }

    private function query_drop_table($name, $ifExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\Z/is", $name, $matches)) {
            $table_name_pieces = $this->parse_relation_name($matches[1]);
            $schema = $this->find_schema($table_name_pieces[0], $table_name_pieces[1]);
            if ($schema == false) {
                return false;
            }

            $table_name = $table_name_pieces[2];
            $table = $schema->getTable($table_name);
            if ($table->isReadLocked()) {
                return $this->error_table_read_lock($table_name_pieces);
            }

            $existed = $schema->dropTable($table_name);
            if (!$ifExists && !$existed) {
                return $this->error_table_not_exists($table_name_pieces);
            }

            return true;
        } else {
            return $this->set_error('Parse error in table listing');
        }
    }

    private function query_drop_database($name, $ifExists)
    {
        if (preg_match("/\A`?([^\W\d]\w*)`?\Z/is", $name, $matches)) {
            $db_name = $matches[1];

            if (!isset($this->databases[$db_name])) {
                if (!$ifExists) {
                    return $this->set_error("Database '{$db_name}' does not exist");
                } else {
                    return true;
                }
            }

            $db = $this->databases[$db_name];
            $db->drop();
            unset($this->databases[$db_name]);

            return true;
        } else {
            return $this->set_error('Parse error in databse listing');
        }
    }

    private function query_drop_schema($name, $ifExists)
    {
        if (preg_match("/\A`?([^\W\d]\w*)`?\Z/is", $name, $matches)) {
            $db_name = $matches[1];

            if (!isset($this->databases[$db_name])) {
                if (!$ifExists) {
                    return $this->set_error("Database '{$db_name}' does not exist");
                } else {
                    return true;
                }
            }

            $db = $this->databases[$db_name];
            $db->drop();
            unset($this->databases[$db_name]);
        } else {
            return $this->set_error('Parse error in schema listing');
        }
    }

    private function query_drop_sequence($name, $ifExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\Z/is", $query, $matches)) {
            list(, $ifExists, $fullSequenceName) = $matches;

            $seqNamePieces = $this->parse_relation_name($fullSequenceName);
            $schema = $this->find_schema($seqNamePieces[0], $seqNamePieces[1]);
            if ($schema == false) {
                return false;
            }

            $sequenceName = $seqNamePieces[2];
            $sequence = $schema->getSequence($sequenceName);

            if ($sequence === false) {
                if (!$ifExists) {
                    return $this->set_error("Sequence {$fullSequenceName} does not exist");
                } else {
                    return true;
                }
            }

            $sequences = $schema->getSequences();
            $sequences->dropSequence($sequenceName);

            return true;
        } else {
            return $this->set_error('Parse error in sequence listing');
        }
    }

    private function query_truncate($query)
    {
        if (preg_match("/\ATRUNCATE\s+TABLE\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(CONTINUE|RESTART)\s+IDENTITY)?\s*[;]?\Z/is", $query, $matches)) {
            $table_name_pieces = $this->parse_relation_name($matches[1]);
            $table = $this->find_table($table_name_pieces);
            if ($table === false) {
                return false;
            } elseif ($table->isReadLocked()) {
                return $this->error_table_read_lock($table_name_pieces);
            }

            $table->truncate();
            if (isset($matches[2]) && !strcasecmp($matches[2], 'RESTART')) {
                $identity = $table->getIdentity();
                if ($identity !== null) {
                    $identity->restart();
                }
            }
        } else {
            return $this->set_error('Invalid TRUNCATE query');
        }

        return true;
    }

    private function query_show($query)
    {
        if (preg_match("/\ASHOW\s+(FULL\s+)?TABLES(?:\s+FROM\s+`?([^\W\d]\w*)`?)?\s*[;]?\Z/is", $query, $matches)) {
            $full = !empty($matches[1]);
            $db_name = !empty($matches[2]) ? $matches[2] : '';

            $db = $this->get_database($db_name);
            if ($db === false) {
                return false;
            }

            $tables = $db->listTables();
            $data = array();

            foreach ($tables as $table_name) {
                if ($full) {
                    $data[] = array($table_name, 'BASE TABLE');
                } else {
                    $data[] = array($table_name);
                }
            }

            $columns = array('Name');
            if ($full) {
                $columns[] = 'Table_type';
            }

            return new ResultSet($columns, $data);
        } elseif (preg_match("/\ASHOW\s+DATABASES\s*[;]?\s*\Z/is", $query, $matches)) {
            foreach ($this->list_dbs() as $db) {
                $data[] = array($db);
            }

            $columns = array('Name');

            return new ResultSet($columns, $data);
        } elseif (preg_match('/\ASHOW\s+(FULL\s+)?COLUMNS\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?(?:\s+(?:FROM|IN)\s+(?:`?([^\W\d]\w*)`?\.`?)?([^\W\d]\w*)`?)?([^\W\d]\w*)`?)?\s*[;]?\s*\Z/is', $query, $matches)) {
            $db_name = !empty($matches[3]) ? $matches[3] : null;
            $schema_name = isset($matches[4]) ? $matches[4] : null;
            $table_name = $matches[2];
            $pieces = array($db_name, $schema_name, $table_name);

            return $this->show_columns($pieces, !empty($matches[1]));
        } else {
            return $this->set_error('Invalid SHOW query');
        }
    }

    private function show_columns($name_pieces, $full)
    {
        $table = $this->find_table($name_pieces);
        if ($table === false) {
            return false;
        }
        $tableColumns = $table->getColumns();

        $data = array();

        foreach ($tableColumns as $name => $column) {
            $type = $this->typecode_to_name($column['type']);
            $null = ($column['null']) ? 'YES' : '';
            $extra = ($column['auto']) ? 'auto_increment' : '';
            $default = $column['default'];

            if (preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $default, $matches)) {
                $default = $matches[1];
            }

            if ($column['key'] == 'p') {
                $key = 'PRI';
            } elseif ($column['key'] == 'u') {
                $key = 'UNI';
            } else {
                $key = '';
            }

            $row = array($name, $type, $null, $column['default'], $key, $extra);
            if ($full) {
                array_splice($row, 2, 0, array(null));
                array_push($row, 'select,insert,update,references', '');
            }

            $data[] = $row;
        }

        $columns = array('Field', 'Type', 'Null', 'Default', 'Key', 'Extra');
        if ($full) {
            array_splice($columns, 2, 0, 'Correlation');
            array_push($columns, 'Privileges', 'Comment');
        }

        return new ResultSet($columns, $data);
    }

    private function query_describe($query)
    {
        if (preg_match("/\ADESC(?:RIBE)?\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s*[;]?\Z/is", $query, $matches)) {
            $name_pieces = $this->parse_relation_name($matches[1]);

            return $this->show_columns($name_pieces, false);
        } else {
            return $this->set_error('Invalid DESCRIBE query');
        }
    }

    private function query_use($query)
    {
        if (preg_match("/\AUSE\s+`?([^\W\d]\w*)`?\s*[;]?\Z/is", $query, $matches)) {
            $this->select_db($matches[1]);

            return true;
        } else {
            return $this->set_error('Invalid USE query');
        }
    }

    private function query_lock($query)
    {
        if (preg_match("/\ALOCK\s+TABLES\s+(.+?)\s*[;]?\Z/is", $query, $matches)) {
            preg_match_all("/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+((?:READ(?:\s+LOCAL)?)|((?:LOW\s+PRIORITY\s+)?WRITE))/is", $matches[1], $rules);
            $numRules = count($rules[0]);
            for ($r = 0; $r < $numRules; ++$r) {
                $table_name_pieces = $this->parse_relation_name($rules[1][$r]);
                $table = $this->find_table($table_name_pieces);
                if ($table !== false) {
                    return false;
                }

                if (!strcasecmp(substr($rules[2][$r], 0, 4), 'READ')) {
                    $table->readLock();
                } else {  /* WRITE */
                    $table->writeLock();
                }

                $lockedTables[] = $table;
            }

            return true;
        } else {
            return $this->set_error('Invalid LOCK query');
        }
    }

    private function query_unlock($query)
    {
        if (preg_match("/\AUNLOCK\s+TABLES\s*[;]?\Z/is", $query)) {
            $this->unlock_tables();

            return true;
        } else {
            return $this->set_error('Invalid UNLOCK query');
        }
    }

    public function parse_value($columnDef, $value)
    {
        // Blank, NULL, or DEFAULT values
        if (!strcasecmp($value, 'NULL') || strlen($value) === 0 || !strcasecmp($value, 'DEFAULT')) {
            return !$columnDef['null'] ? $columnDef['default'] : null;
        }

        switch ($columnDef['type']) {
            case Types::INTEGER:
                if (preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
                    return (int) $matches[1];
                } elseif (preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
                    return (int) $value;
                } else {
                    return $this->set_error('Invalid integer value for insert');
                }
            case Types::FLOAT:
                if (preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
                    return (float) $matches[1];
                } elseif (preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
                    return (float) $value;
                } else {
                    return $this->set_error('Invalid float value for insert');
                }
            case Types::ENUM:
                if (preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $matches)) {
                    $value = $matches[1];
                }

                if (in_array($value, $columnDef['restraint']) || strlen($value) === 0) {
                    return $value;
                } elseif (is_numeric($value)) {
                    $index = (int) $value;
                    if ($index >= 1 && $index <= count($columnDef['restraint'])) {
                        return $columnDef['restraint'][$index - 1];
                    } elseif ($index === 0) {
                        return '';
                    } else {
                        return $this->set_error('Numeric ENUM value out of bounds');
                    }
                } else {
                    return $columnDef['default'];
                }
            case Types::DATE:
                list($year, $month, $day) = array('0000', '00', '00');
                if (preg_match("/\A'((?:[1-9]\d)?\d{2})-(0[1-9]|1[0-2])-([0-2]\d|3[0-1])(?: (?:[0-1]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d))?'\Z/is", $value, $matches)
                || preg_match("/\A'((?:[1-9]\d)?\d{2})(0[1-9]|1[0-2])([0-2]\d|3[0-1])(?:(?:[0-1]\d|2[0-3])(?:[0-5]\d)(?:[0-5]\d))?'\Z/is", $value, $matches)) {
                    list(, $year, $month, $day) = $matches;
                } else {
                    list($year, $month, $day) = array('0000', '00', '00');
                }
                if (strlen($year) === 2) {
                    $year = ($year <= 69) ? 2000 + $year : 1900 + $year;
                }

                return $year.'-'.$month.'-'.$day;
            default:
                if (preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $matches)) {
                    return (string) $matches[1];
                } else {
                    return $value;
                }
        }

        return false;
    }

    private function build_parameters($join_info, $where_type, $paramList)
    {
        $params = array();
        array_shift($paramList);
        foreach ($paramList as $match) {
            $expr = $this->build_expression($match, $join_info, $where_type);
            if ($expr === false) { // parse error
                return false;
            }
            $params[] = $expr;
        }

        return $params;
    }

    private function parse_extract_function($join_info, $where_type, $paramsStr)
    {
        if (preg_match("/\A\s*(\w+)\s+FROM\s+(.+?)\s*\Z/is", $paramsStr, $matches)) {
            $field = strtolower($matches[1]);
            $field = "'$field'";
            $expr = $this->build_expression($matches[2], $join_info, $where_type);
            if ($expr !== false) {
                return array('extract', array($field, $expr));
            }
        }

        return $this->set_error('Error parsing extract() function parameters');
    }

    private function parse_overlay_function($join_info, $where_type, $paramsStr)
    {
        if (preg_match("/\A\s*(.+?)\s+PLACING\s+(.+?)\s+FROM\s+(.+?)(?:\s+FOR\s+(.+?))?\s*\Z/is", $paramsStr, $matches)) {
            $params = $this->build_parameters($join_info, $where_type, $matches);

            return $params !== false ? array('overlay', $params) : false;
        } else {
            return $this->set_error('Error parsing overlay() function parameters');
        }
    }

    private function parse_position_function($join_info, $where_type, $params)
    {
        if (preg_match("/\A\s*(.+?)\s+IN\s+(.+?)\s*\Z/is", $params, $matches)) {
            $substring = $this->build_expression($matches[1], $join_info, $where_type);
            $string = $this->build_expression($matches[2], $join_info, $where_type);
            if ($substring !== false && $string !== false) {
                return array('position', array($substring, $string));
            }
        }

        return $this->set_error('Error parsing position() function parameters');
    }

    private function parse_substring_function($join_info, $where_type, $paramsStr)
    {
        if (preg_match("/\A\s*(.+?)\s+FROM\s+(.+?)(?:\s+FOR\s+(.+?))?\s*\Z/is", $paramsStr, $matches)) {
            $params = $this->build_parameters($join_info, $where_type, $matches);

            return $params !== null ? array('substring', $params) : false;
        } else {
            return $this->set_error('Error parsing substring() function parameters');
        }
    }

    private function parse_trim_function($join_info, $where_type, $paramsStr)
    {
        if (preg_match("/\A\s*(?:(?:(LEADING|TRAILING|BOTH)\s+)?(?:(.+?)\s+)?FROM\s+)?(.+?)\s*\Z/is", $paramsStr, $matches)) {
            switch (strtoupper($matches[1])) {
                case 'LEADING':
                    $function = 'ltrim';
                    break;
                case 'TRAILING':
                    $function = 'rtrim';
                    break;
                default:
                    $function = 'trim';
                    break;
            }

            $string = $this->build_expression($matches[3], $join_info, $where_type);
            if ($string === null) {
                return false;
            }
            $params = array($string);

            if (!empty($matches[2])) {
                $characters = $this->build_expression($matches[2], $join_info, $where_type);
                if ($characters === null) {
                    return false;
                }
                $params[] = $characters;
            }

            return array($function, $params);
        } else {
            return $this->set_error('Error parsing trim() function parameters');
        }
    }

    private function inner_join($left_data, $right_data, $join_comparator)
    {
        if (empty($left_data) || empty($right_data)) {
            return array();
        }

        $new_join_data = array();

        foreach ($left_data as $left_entry) {
            foreach ($right_data as $right_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }
        }

        return $new_join_data;
    }

    private function left_join($left_data, $right_data, $join_comparator, $pad_length, &$joinMatches)
    {
        $new_join_data = array();
        $right_padding = array_fill(0, $pad_length, null);

        foreach ($left_data as $left_row => $left_entry) {
            $match_found = false;
            foreach ($right_data as $right_row => $right_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $joinMatches[$left_row] = $right_row;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }

            if (!$match_found) {
                $new_join_data[] = array_merge($left_entry, $right_padding);
                $joinMatches[$left_row] = false;
            }
        }

        return $new_join_data;
    }

    private function right_join($left_data, $right_data, $join_comparator, $pad_length)
    {
        $new_join_data = array();
        $left_padding = array_fill(0, $pad_length, null);

        foreach ($right_data as $right_entry) {
            $match_found = false;
            foreach ($left_data as $left_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }

            if (!$match_found) {
                $new_join_data[] = array_merge($left_padding, $right_entry);
            }
        }

        return $new_join_data;
    }

    private function full_join($left_data, $right_data, $join_comparator, $left_pad_length, $right_pad_length)
    {
        $new_join_data = array();
        $matched_rids = array();
        $left_padding = array_fill(0, $left_pad_length, null);
        $right_padding = array_fill(0, $right_pad_length, null);

        foreach ($left_data as $left_entry) {
            $match_found = false;
            foreach ($right_data as $rid => $right_entry) {
                if ($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                    if (!in_array($rid, $matched_rids)) {
                        $matched_rids[] = $rid;
                    }
                }
            }

            if (!$match_found) {
                $new_join_data[] = array_merge($left_entry, $right_padding);
            }
        }

        $unmatched_rids = array_diff(array_keys($right_data), $matched_rids);
        foreach ($unmatched_rids as $rid) {
            $new_join_data[] = array_merge($left_padding, $right_data[$rid]);
        }

        return $new_join_data;
    }

    public function fetch_all(ResultSet $results, $type = ResultSet::FETCH_ASSOC)
    {
        return $results->fetchAll($type);
    }

    public function fetch_array(ResultSet $results, $type = ResultSet::FETCH_ASSOC)
    {
        return $results->fetchArray($type);
    }

    public function fetch_assoc(ResultSet $results)
    {
        return $results->fetchAssoc();
    }

    public function fetch_row(ResultSet $results)
    {
        return $results->fetchRow();
    }

    public function fetch_both(ResultSet $results)
    {
        return $results->fetchBoth();
    }

    public function fetch_single(ResultSet $results, $column = 0)
    {
        return $results->fetchSingle($column);
    }

    public function fetch_object(ResultSet $results)
    {
        return $results->fetchObject();
    }

    public function data_seek(ResultSet $results, $i)
    {
        return $results->dataSeek($i);
    }

    public function num_rows(ResultSet $results)
    {
        return $results->numRows();
    }

    public function num_fields(ResultSet $results)
    {
        return $results->numFields();
    }

    public function fetch_field(ResultSet $results)
    {
        return $results->fetchField();
    }

    public function field_seek(ResultSet $results, $i)
    {
        return $results->fieldSeek($i);
    }

    public function free_result(ResultSet $results)
    {
        // No-op for backwards compat
    }

    private function typecode_to_name($type)
    {
        switch ($type) {
            case Types::DATE:                return 'DATE';
            case Types::DATETIME:            return 'DATETIME';
            case Types::ENUM:                return 'ENUM';
            case Types::FLOAT:               return 'DOUBLE';
            case Types::INTEGER:             return 'INTEGER';
            case Types::STRING:              return 'TEXT';
            case Types::TIME:                return 'TIME';
            default:                         return false;
        }
    }
}
