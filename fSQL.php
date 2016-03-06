<?php

define("FSQL_ASSOC",1,true);
define("FSQL_NUM",  2,true);
define("FSQL_BOTH", 3,true);

define("FSQL_EXTENSION", ".cgi",true);

define('FSQL_TYPE_DATE','d',true);
define('FSQL_TYPE_DATETIME','dt',true);
define('FSQL_TYPE_ENUM','e',true);
define('FSQL_TYPE_FLOAT','f',true);
define('FSQL_TYPE_INTEGER','i',true);
define('FSQL_TYPE_STRING','s',true);
define('FSQL_TYPE_TIME','t',true);

define('FSQL_WHERE_NORMAL',2,true);
define('FSQL_WHERE_NORMAL_AGG',3,true);
define('FSQL_WHERE_ON',4,true);
define('FSQL_WHERE_HAVING',8,true);
define('FSQL_WHERE_HAVING_AGG',9,true);

define('FSQL_TRUE', 3, true);
define('FSQL_FALSE', 0,true);
define('FSQL_NULL', 1,true);
define('FSQL_UNKNOWN', 1,true);

if(!defined('FSQL_INCLUDE_PATH')) {
    define('FSQL_INCLUDE_PATH', dirname(__FILE__));
}

require_once FSQL_INCLUDE_PATH.'/fSQLUtilities.php';
require_once FSQL_INCLUDE_PATH.'/fSQLDatabase.php';
require_once FSQL_INCLUDE_PATH.'/fSQLFunctions.php';

class fSQLEnvironment
{
    private $updatedTables = array();
    private $lockedTables = array();
    private $databases = array();
    private $currentDB = null;
    private $error_msg = null;
    private $query_count = 0;
    private $cursors = array();
    private $columns = array();
    private $data = array();
    private $join_lambdas = array();
    private $affected = 0;
    private $insert_id = 0;
    private $auto = 1;
    private $functions;

    public function __construct()
    {
        $this->functions = new fSQLFunctions($this);
    }

    public function __destruct()
    {
        $this->unlock_tables();
    }

    public function define_db($name, $path)
    {
        $path = realpath($path);
        if($path === false || !is_dir($path)) {
            if(@mkdir($path, 0777))
                $path = realpath($path);
        } else if(!is_readable($path) || !is_writeable($path)) {
            chmod($path, 0777);
        }

        if($path && substr($path, -1) != '/')
            $path .= '/';

        list($usec, $sec) = explode(' ', microtime());
        srand((float) $sec + ((float) $usec * 100000));

        if(is_dir($path) && is_readable($path) && is_writeable($path)) {
            $db = new fSQLDatabase($name, $path);
            $this->databases[$name] = $db;
            return true;
        } else {
            return $this->set_error("Path to directory for {$name} database is not valid.  Please correct the path or create the directory and chmod it to 777.");
        }
    }

    public function select_db($name)
    {
        if(isset($this->databases[$name])) {
            $this->currentDB = $this->databases[$name];
            return true;
        } else {
            return $this->set_error("No database called {$name} found");
        }
    }

    public function current_db()
    {
        return $this->currentDB;
    }

    public function error()
    {
        return $this->error_msg;
    }

    public function register_function($sqlName, $phpName)
    {
        return $this->functions->register($sqlName, $phpName);
    }

    private function set_error($error)
    {
        $this->error_msg = $error."\r\n";
        return false;
    }

    private function error_table_not_exists($tableFullName)
    {
        return $this->set_error("Table {$tableFullName} does not exist");
    }

    private function error_table_read_lock($tableFullName)
    {
        return $this->set_error("Table {$tableFullName} is locked for reading only");
    }

    public function get_database($db_name)
    {
        $db = false;

        if(!$db_name) {
            if($this->currentDB !== null) {
                $db = $this->currentDB;
            } else {
                $this->set_error('No database specified');
            }
        } else {
            // if database $db_name is not defined set error
            if(isset($this->databases[$db_name])) {
                $db = $this->databases[$db_name];
            } else {
                $this->set_error("Database $db_name not found");
            }
        }

        return $db;
    }

    public function find_table($dbName, $tableName)
    {
        $table = false;

        $db = $this->get_database($dbName);
        if($db !== false) {
            $table = $db->getTable($tableName);
            if(!$table->exists()) {
                $this->error_table_not_exists($table->fullName());
                $table = false;
            }
        }

        return $table;
    }

    public function lookup_function($function)
    {
        $match = $this->functions->lookup($function);
        if($match === false) {
            return $this->set_error("Call to unknown SQL function");
        }
        return $match;
    }

    public function escape_string($string)
    {
        return str_replace(array("\\", "\0", "\n", "\r", "\t", "'"), array("\\\\", "\\0", "\\n", "\\", "\\t", "\\'"), $string);
    }

    public function affected_rows()
    {
        return $this->affected;
    }

    public function insert_id()
    {
        return $this->insert_id;
    }

    public function num_rows($id)
    {
        if(isset($this->data[$id])) {
            return count($this->data[$id]);
        } else {
            return 0;
        }
    }

    public function query_count()
    {
        return $this->query_count;
    }

    private function unlock_tables()
    {
        foreach (array_keys($this->lockedTables) as $index )
            $this->lockedTables[$index]->unlock();
        $this->lockedTables = array();
    }

    private function begin()
    {
        $this->auto = 0;
        $this->unlock_tables();
        $this->commit();
    }

    private function commit()
    {
        $this->auto = 1;
        foreach (array_keys($this->updatedTables) as $index ) {
            $this->updatedTables[$index]->commit();
        }
        $this->updatedTables = array();
    }

    private function rollback()
    {
        $this->auto = 1;
        foreach (array_keys($this->updatedTables) as $index ) {
            $this->updatedTables[$index]->rollback();
        }
        $this->updatedTables = array();
    }

    public function query($query)
    {
        $query = trim($query);
        list($function, ) = explode(" ", $query);
        $this->query_count++;
        $this->error_msg = null;
        switch(strtoupper($function)) {
            case 'CREATE':        return $this->query_create($query);
            case 'SELECT':        return $this->query_select($query);
            case 'INSERT':
            case 'REPLACE':    return $this->query_insert($query);
            case 'UPDATE':        return $this->query_update($query);
            case 'ALTER':        return $this->query_alter($query);
            case 'DELETE':        return $this->query_delete($query);
            case 'BEGIN':        return $this->query_begin($query);
            case 'START':        return $this->query_start($query);
            case 'COMMIT':        return $this->query_commit($query);
            case 'ROLLBACK':    return $this->query_rollback($query);
            case 'RENAME':    return $this->query_rename($query);
            case 'TRUNCATE':    return $this->query_truncate($query);
            case 'DROP':        return $this->query_drop($query);
            case 'BACKUP':        return $this->query_backup($query);
            case 'RESTORE':    return $this->query_restore($query);
            case 'USE':        return $this->query_use($query);
            case 'DESC':
            case 'DESCRIBE':    return $this->query_describe($query);
            case 'SHOW':        return $this->query_show($query);
            case 'LOCK':        return $this->query_lock($query);
            case 'UNLOCK':        return $this->query_unlock($query);
            case 'MERGE':        return $this->query_merge($query);
            default:            $this->set_error('Invalid Query');  return false;
        }
    }

    private function query_begin($query)
    {
        if(preg_match("/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is", $query, $matches)) {
            $this->begin();
            return true;
        } else {
            return $this->set_error('Invalid BEGIN query');
        }
    }

    private function query_start($query)
    {
        if(preg_match("/\ASTART\s+TRANSACTION\s*[;]?\Z/is", $query, $matches)) {
            $this->begin();
            return true;
        } else {
            return $this->set_error('Invalid START query');
        }
    }

    private function query_commit($query)
    {
        if(preg_match("/\ACOMMIT\s*[;]?\Z/is", $query, $matches)) {
            $this->commit();
            return true;
        } else {
            return $this->set_error('Invalid COMMIT query');
        }
    }

    private function query_rollback($query)
    {
        if(preg_match("/\AROLLBACK\s*[;]?\Z/is", $query, $matches)) {
            $this->rollback();
            return true;
        } else {
            return $this->set_error('Invalid ROLLBACK query');
        }
    }

    private function query_create($query)
    {
        if(preg_match("/\ACREATE(?:\s+TEMPORARY)?\s+TABLE\s+/is", $query)) {
            return $this->query_create_table($query);
        } else if(preg_match("/\ACREATE\s+SEQUENCE\s+/is", $query)) {
            return $this->query_create_sequence($query);
        } else {
            return $this->set_error('Invalid CREATE query');
        }
    }

    private function query_create_sequence($query)
    {

        if(preg_match("/\ACREATE\s+SEQUENCE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $ifNotExists, $dbName, $sequenceName, $valuesList) = $matches;
            $db = $this->get_database($dbName);
            if($db === false) {
                return false;
            }

            $sequences = $db->getSequences();
            $seqFileExists = $sequences->exists();
            if($seqFileExists) {
                $sequence = $sequences->getSequence($sequenceName);
                if($sequence !== false) {
                    if(empty($ifNotExists)) {
                        return $this->set_error("Sequence {$db->name()}.{$sequenceName} already exists");
                    } else {
                        return true;
                    }
                }
            }

            $parsed = $this->parse_sequence_options($valuesList);
            if($parsed === false) {
                return false;
            }

            list($start, $increment, $min, $max, $cycle) = $this->load_create_sequence($parsed);

            if(!$seqFileExists) {
                $sequences->create();
            }
            $sequences->addSequence($sequenceName, $start, $increment, $min, $max, $cycle);

            return true;
        } else {
            return $this->set_error('Invalid CREATE SEQUENCE query');
        }
    }

    private function query_create_table($query)
    {
        if(preg_match("/\ACREATE(?:\s+(TEMPORARY))?\s+TABLE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*?)`?(?:\s*\((.+)\)|\s+LIKE\s+(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*))/is", $query, $matches)) {

            list(, $temporary, $ifnotexists, $db_name, $table_name, $column_list) = $matches;

            if(!$table_name) {
                return $this->set_error("No table name specified");
            }

            $db = $this->get_database($db_name);
            if($db === false) {
                return false;
            }

            $table = $db->getTable($table_name);
            if($table->exists()) {
                if(empty($ifnotexists)) {
                    return $this->set_error("Table {$db->name}.{$table_name} already exists");
                } else {
                    return true;
                }
            }

            $temporary = !empty($temporary) ? true : false;

            if(!isset($matches[6])) {
                //preg_match_all("/(?:(KEY|PRIMARY KEY|UNIQUE) (?:([A-Z][A-Z0-9\_]*)\s*)?\((.+?)\))|(?:`?([A-Z][A-Z0-9\_]*?)`?(?:\s+((?:TINY|MEDIUM|BIG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)(\s+UNSIGNED)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);
                preg_match_all("/(?:(?:CONSTRAINT\s+(?:`?[A-Z][A-Z0-9\_]*`?\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+`?([A-Z][A-Z0-9\_]*)`?)?\s*\(`?(.+?)`?\))|(?:`?([A-Z][A-Z0-9\_]*?)`?(?:\s+((?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)\s*(UNSIGNED\s+)?(?:GENERATED\s+(BY\s+DEFAULT|ALWAYS)\s+AS\s+IDENTITY(?:\s*\((.*?)\))?)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);

                if(!$Columns) {
                    return $this->set_error("Parsing error in CREATE TABLE query");
                }

                $new_columns = array();
                $hasIdentity = false;

                for($c = 0; $c < count($Columns[0]); $c++) {
                    //$column = str_replace("\"", "'", $column);
                    if($Columns[1][$c])
                    {
                        if(!$Columns[3][$c]) {
                            return $this->set_error("Parse Error: Excepted column name in \"{$Columns[1][$c]}\"");
                        }

                        $keytype = strtolower($Columns[1][$c]);
                        if($keytype === "index")
                            $keytype = "key";
                        $keycolumns = explode(",", $Columns[3][$c]);
                        foreach($keycolumns as $keycolumn)
                        {
                            $new_columns[trim($keycolumn)]['key'] = $keytype[0];
                        }
                    }
                    else
                    {
                        $name = $Columns[4][$c];
                        $type = $Columns[5][$c];
                        $options =  $Columns[10][$c];

                        if(isset($new_columns[$name])) {
                            return $this->set_error("Column '{$name}' redefined");
                        }

                        $type = strtoupper($type);
                        if(in_array($type, array('CHAR', 'VARCHAR', 'BINARY', 'VARBINARY', 'TEXT', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT', 'SET', 'BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB'))) {
                            $type = FSQL_TYPE_STRING;
                        } else if(in_array($type, array('BIT','TINYINT', 'SMALLINT','MEDIUMINT','INT','INTEGER','BIGINT'))) {
                            $type = FSQL_TYPE_INTEGER;
                        } else if(in_array($type, array('FLOAT','REAL','DOUBLE','DOUBLE PRECISION','NUMERIC','DEC','DECIMAL'))) {
                            $type = FSQL_TYPE_FLOAT;
                        } else {
                            switch($type)
                            {
                                case 'DATETIME':
                                    $type = FSQL_TYPE_DATETIME;
                                    break;
                                case 'DATE':
                                    $type = FSQL_TYPE_DATE;
                                    break;
                                case 'ENUM':
                                    $type = FSQL_TYPE_ENUM;
                                    break;
                                case 'TIME':
                                    $type = FSQL_TYPE_TIME;
                                    break;
                                default:
                                    break;
                            }
                        }

                        if(preg_match("/not\s+null/i", $options))
                            $null = 0;
                        else
                            $null = 1;

                        $auto = 0;
                        $restraint = null;
                        if(!empty($Columns[8][$c])) {
                            $auto = 1;
                            $always = (int) !strcasecmp($Columns[8][$c], 'ALWAYS');
                            $parsed = $this->parse_sequence_options($Columns[9][$c]);
                            if($parsed === false) {
                                return false;
                            }

                            $restraint = $this->load_create_sequence($parsed);
                            $start = $restraint[0];
                            array_unshift($restraint, $start, $always);

                            $null = 0;
                        }
                        else if(preg_match('/\s+AUTO_?INCREMENT\b/i', $options))
                        {
                            $auto = 1;
                            $intMax = defined('PHP_INT_MAX') ? PHP_INT_MAX : intval('420000000000000000000');
                            $restraint = array(1, 0, 1, 1, 1, $intMax, 0);
                        }

                        if($auto) {
                            if($type !== FSQL_TYPE_INTEGER && $type !== FSQL_TYPE_FLOAT) {
                                return $this->set_error("Identity columns and autoincrement only allowed on numeric columns");
                            } else if($hasIdentity) {
                                return $this->set_error("A table can only have one identity column.");
                            }
                            $hasIdentity = true;
                        }

                        if($type === FSQL_TYPE_ENUM) {
                            preg_match_all("/'(.*?(?<!\\\\))'/", $Columns[6][$c], $values);
                            $restraint = $values[1];
                        }

                        if(preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')/is", $options, $matches)) {
                            if($auto) {
                                return $this->set_error("Can not specify a default value for an identity column");
                            }

                            $default = $this->parseDefault($matches[1], $type, $null, $restraint);
                        } else if($null) {
                            $default = "NULL";
                        } else if($type === FSQL_TYPE_STRING) {
                            $default = '';
                        } else if($type === FSQL_TYPE_FLOAT) {
                            $default = 0.0;
                        } else {
                            // The default for dates, times, enums, and int types is 0
                            $default = 0;
                        }

                        if(preg_match("/(PRIMARY KEY|UNIQUE(?: KEY)?)/is", $options, $keymatches)) {
                            $keytype = strtolower($keymatches[1]);
                            $key = $keytype{0};
                        }
                        else {
                            $key = "n";
                        }

                        $new_columns[$name] = array('type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint);
                    }
                }
            } else {
                $src_db_name = $matches[6];
                $src_table_name = $matches[7];

                $src_table = $this->find_table($src_db_name, $src_table_name);
                if($src_table !== false) {
                    $new_columns = $src_table->getColumns();
                } else {
                    return false;
                }
            }

            $db->createTable($table_name, $new_columns, $temporary);

            return true;
        } else {
            return $this->set_error('Invalid CREATE TABLE query');
        }
    }

    private function load_create_sequence($parsed)
    {
        $increment = isset($parsed['INCREMENT']) ? (int) $parsed['INCREMENT'] : 1;
        if($increment === 0)
            return $this->set_error('Increment of zero in identity column defintion is not allowed');

        $intMax = defined('PHP_INT_MAX') ? PHP_INT_MAX : intval('420000000000000000000');
        $intMin = defined('PHP_INT_MIN') ? PHP_INT_MIN : ~$intMax;

        $climbing = $increment > 0;
        $min = isset($parsed['MINVALUE']) ? (int) $parsed['MINVALUE'] : ($climbing ? 1 : $intMin);
        $max = isset($parsed['MAXVALUE']) ? (int) $parsed['MAXVALUE'] : ($climbing ? $intMax : -1);
        $cycle = isset($parsed['CYCLE']) ? (int) $parsed['CYCLE'] : 0;

        if(isset($parsed['START'])) {
            $start = (int) $parsed['START'];
            if($start < $min || $start > $max) {
                return $this->set_error('Identity column start value not inside valid range');
            }
        } else if($climbing) {
            $start = $min;
        } else {
            $start = $max;
        }

        return array($start, $increment, $min, $max, $cycle);
    }

    private function parse_sequence_options($options, $isAlter = false)
    {
        $parsed = array();
        if(!empty($options)) {
            if(!$isAlter) {
                $startName = 'START';
            } else {
                $startName = 'RESTART';
            }

            $valueTypes = array($startName, 'INCREMENT', 'MINVALUE', 'MAXVALUE');
            $secondWords = array($startName => "WITH", 'INCREMENT' => 'BY');
            $startKey = $startName.'WITH';
            $optionsWords = preg_split('/\s+/', strtoupper($options));
            $wordCount = count($optionsWords);
            for($i = 0; $i < $wordCount; $i++) {
                $word = $optionsWords[$i];
                if($isAlter) {
                    if($word === 'SET') {
                        $word = $optionsWords[++$i];
                        if(!in_array($word, array('INCREMENT', 'CYCLE', 'MAXVALUE', 'MINVALUE', 'GENERATED'))) {
                            return $this->set_error('Unknown option after SET: '.$word);
                        }
                    }

                    if($word === 'RESTART') {
                        if(($i + 1) == $wordCount || $optionsWords[$i + 1] !== 'WITH')
                        {
                            $parsed['RESTART'] = "start";
                            continue;
                        }
                    }

                    if($word === 'GENERATED') {
                        $word = $optionsWords[++$i];
                        if($word === 'BY') {
                            $word = $optionsWords[++$i];
                            if($word !== 'DEFAULT') {
                                return $this->set_error('Expected DEFAULT after BY');
                            }
                            $parsed['ALWAYS'] = false;
                        } else if($word === 'ALWAYS') {
                            $parsed['ALWAYS'] = true;
                        } else {
                            return $this->set_error('Unexpected word after GENERATED: ' + $word);
                        }
                    }
                }

                if(in_array($word, $valueTypes)) {
                    $original = $word;
                    if(isset($secondWords[$original])) {
                        $word = $optionsWords[++$i];
                        $second = $secondWords[$original];
                        if($word !== $second) {
                            return $this->set_error('Expected '.$second .' after '.$original);
                        }
                    }

                    $word = $optionsWords[++$i];
                    if(preg_match('/[+-]?\s*\d+(?:\.\d+)?/', $word, $number)) {
                        if(!isset($parsed[$original])) {
                            $parsed[$original] = $number[0];
                        } else {
                            return $this->set_error($original.' already set for this identity/sequence.');
                        }
                    } else {
                        return $this->set_error('Could not parse number after '.$original);
                    }

                } else if($word === 'NO') {
                    $word = $optionsWords[++$i];
                    if(in_array($word, array('CYCLE', 'MAXVALUE', 'MINVALUE'))) {
                        if(!isset($parsed[$word])) {
                            $parsed[$word] = null;
                        } else {
                            return $this->set_error($word.' already set for this identity column.');
                        }
                    } else {
                        return $this->set_error('Unknown option after NO: '.$word);
                    }
                } else if($word === 'CYCLE') {
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
        if(preg_match("/\A((INSERT|REPLACE)(?:\s+(IGNORE))?\s+INTO\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?)\s+(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $beginning, $command, $ignore, $db_name, $table_name, $the_rest) = $matches;
        } else {
            return $this->set_error('Invalid Query');
        }

        // INSERT...SELECT
        if(preg_match("/^SELECT\s+.+/is", $the_rest)) {
            $id = $this->query_select($the_rest);
            while($values = fsql_fetch_array($id)) {
                $this->query_count--;
                $this->query_insert($beginning." VALUES('".join("', '", $values)."')");
            }
            fsql_free_result($id);
            unset ($id, $values);
            return true;
        }

        $db = $this->get_database($db_name);
        if($db === false) {
            return false;
        }

        $table = $this->find_table($db_name, $table_name);
        if($table === false) {
            return false;
        } else if($table->isReadLocked()) {
            return $this->error_table_read_lock($table-fullName());
        }

        $tableColumns = $table->getColumns();
        $tableCursor = $table->getCursor();

        $check_names = 1;
        $replace = !strcasecmp($command, 'REPLACE');

        // Column List present and VALUES list
        if(preg_match("/^\(`?(.+?)`?\)\s+VALUES\s*\((.+)\)/is", $the_rest, $matches)) {
            $Columns = preg_split("/`?\s*,\s*`?/s", $matches[1]);
            $get_data_from = $matches[2];
        }
        // VALUES list but no column list
        else if(preg_match("/^VALUES\s*\((.+)\)/is", $the_rest, $matches)) {
            $get_data_from = $matches[1];
            $Columns = $table->getColumnNames();
            $check_names = 0;
        }
        // SET syntax
        else if(preg_match("/^SET\s+(.+)/is", $the_rest, $matches)) {
            $SET = $this->parseSetClause($matches[1], $tableColumns);
            $Columns= array();
            $data_values = array();

            foreach($SET as $set) {
                $Columns[] = $set[0];
                $data_values[] = $set[1];
            }

            $get_data_from = implode(",", $data_values);
        } else {
            return $this->set_error('Invalid Query');
        }

        preg_match_all("/\s*(DEFAULT|AUTO|NULL|'.*?(?<!\\\\)'|(?:[\+\-]\s*)?\d+(?:\.\d+)?|[^$])\s*(?:$|,)/is", $get_data_from, $newData);
        $dataValues = $newData[1];

        if($check_names == 1) {
            $i = 0;
            $TableColumns = $table->getColumnNames();

            if(count($dataValues) != count($Columns)) {
                return $this->set_error("Number of inserted values and columns not equal");
            }

            foreach($Columns as $col_name) {
                if(!in_array($col_name, $TableColumns)) {
                    return $this->set_error("Invalid column name '{$col_name}' found");
                } else {
                    $Data[$col_name] = $dataValues[$i++];
                }
            }

            if(count($Columns) != count($TableColumns)) {
                foreach($TableColumns as $col_name) {
                    if(!in_array($col_name, $Columns)) {
                        $Data[$col_name] = "NULL";
                    }
                }
            }
        }
        else
        {
            $countData = count($dataValues);
            $countColumns = count($Columns);

            if($countData < $countColumns) {
                $Data = array_combine($Columns, array_pad($dataValues, $countColumns, "NULL"));
            } else if($countData > $countColumns) {
                return $this->set_error("Trying to insert too many values");
            } else {
                $Data = array_combine($Columns, $dataValues);
            }
        }

        $newentry = array();

        ////Load Columns & Data for the Table
        $colIndex = 0;
        foreach($tableColumns as $col_name => $columnDef)  {

            unset($delete);

            $data = trim($Data[$col_name]);
            $data = strtr($data, array("$" => "\$", "\$" => "\\\$"));

            ////Check for Auto_Increment
            if($columnDef['auto'] == 1) {
                $identity = $table->getIdentity();
                if(empty($columnDef['restraint'])) {  // upgrade old AUTOINCREMENT column to IDENTITY
                    $always = false;
                    $increment = 1;
                    $min = 1;
                    $max = defined('PHP_INT_MAX') ? PHP_INT_MAX : intval('420000000000000000000');
                    $cycle = false;

                    $entries = $table->getEntries();
                    $max = $this->functions->max($entries, $colIndex, "");
                    if($max !== null)
                        $insert_id = $max + 1;
                    else
                        $insert_id = 1;
                    $this->insert_id = $insert_id;
                    $newentry[$colIndex] = $this->insert_id;

                    $tableColumns[$col_name]['restraint'] = array($insert_id, $always, $min, $increment, $min, $max, $cycle);
                    $table->setColumns($tableColumns);
                } else if(empty($data) || !strcasecmp($data, "AUTO") || !strcasecmp($data, "NULL")) {
                    $insert_id = $identity->nextValueFor();
                    if($insert_id !== false)
                    {
                        $this->insert_id = $insert_id;
                        $newentry[$colIndex] = $this->insert_id;
                    } else {
                        return $this->set_error('Error getting next value for identity column: '.$col_name);
                    }
                } else {
                    if($identity->getAlways()) {
                        return $this->set_error("Manual value inserted into an ALWAYS identity column");
                    }
                    $data = $this->parse_value($columnDef, $data);
                    if($data === false) {
                        return false;
                    }
                    $newentry[$colIndex] = $data;
                }
            }
            ///Check for NULL Values
            else if((!strcasecmp($data, "NULL") && !$columnDef['null']) || empty($data) || !strcasecmp($data, "DEFAULT")) {
                $newentry[$colIndex] = $columnDef['default'];
            } else {
                $data = $this->parse_value($columnDef, $data);
                if($data === false)
                    return false;
                $newentry[$colIndex] = $data;
            }

            ////See if it is a PRIMARY KEY or UNIQUE
            if($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
                if($replace) {
                    $delete = array();
                    $tableCursor->first();
                    $n = 0;
                    while(!$tableCursor->isDone()) {
                        $row = $tableCursor->getRow();
                        if($row[$colIndex] == $newentry[$colIndex]) { $delete[] = $n; }
                        $tableCursor->next();
                        $n++;
                    }
                    if(!empty($delete)) {
                        foreach($delete as $d) {
                            $this->affected++;
                            $table->deleteRow($d);
                        }
                    }
                } else {
                    $tableCursor->first();
                    while(!$tableCursor->isDone()) {
                        $row = $tableCursor->getRow();
                        if($row[$colIndex] == $newentry[$colIndex]) {
                            if(empty($ignore)) {
                                return $this->set_error("Duplicate value for unique column '{$col_name}'");
                            } else {
                                return true;
                            }
                        }
                        $tableCursor->next();
                    }
                }
            }

            $colIndex++;
        }

        $table->insertRow($newentry);

        if($this->auto)
            $table->commit();
        else if(!in_array($table, $this->updatedTables))
            $this->updatedTables[] = $table;

        $this->affected++;

        return true;
    }

    ////Update data in the DB
    private function query_update($query) {
        $this->affected = 0;
        if(preg_match("/\AUPDATE(?:\s+(IGNORE))?\s+(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?\s*[;]?\Z/is", $query, $matches)) {
            $matches[4] = preg_replace("/(.+?)(\s+WHERE)(.*)/is", "\\1", $matches[4]);
            $ignore = !empty($matches[1]);
            $table_name = $matches[3];

            $table = $this->find_table($db_name, $table_name);
            if($table === false) {
                return false;
            } else if($table->isReadLocked()) {
                return $this->error_table_read_lock($table-fullName());
            }

            $columns = $table->getColumns();
            $columnNames = array_keys($columns);
            $cursor = $table->getCursor();
            $col_indicies = array_flip($columnNames);

            $SET = $this->parseSetClause($matches[4], $columns);

            $where = null;
            if(isset($matches[5]))
            {
                $where = $this->build_where($matches[5], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
                if(!$where) {
                    return $this->set_error('Invalid WHERE clause: '.$this->error_msg);
                }
            }

            $updates = array();
            foreach($SET as $set) {
                list($column, $value) = $set;
                $new_value = $this->parse_value($columns[$column], $value);
                if($new_value === false)
                    return false;
                $col_index = $col_indicies[$column];
                $updates[$col_index] = "$col_index => $new_value";
            }

            $unique_key_columns = array();
            foreach($columns as $column => $columnDef) {
                if($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
                    $unique_key_columns[] = $col_indicies[$column];
                }
            }

            $affected = 0;
            $updated_key_columns = array_intersect(array_keys($updates), $unique_key_columns);
            $key_lookup = array();
            if(!empty($updated_key_columns)) {
                for( $rowId = $cursor->first(); !$cursor->isDone(); $rowId = $cursor->next())
                {
                    $entry = $cursor->getRow();
                    foreach($updated_key_columns as $unique) {
                        if(!isset($key_lookup[$unique])) {
                            $key_lookup[$unique] = array();
                        }

                        $key_lookup[$unique][$entry[$unique]] = $rowId;
                    }
                }
            }

            $updates = "array(".implode(',', $updates).")";
            $line = "\t\t\$table->updateRow(\$rowId, \$updates);\r\n";
            $line .= "\t\t\$affected++;\r\n";

            // find all updated columns that are part of a unique key
            // if there are any, call checkUnique to validate still unique.
            $code = '';
            if(!empty($updated_key_columns)) {
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

            if($where)
                $code = "\tif({$where}) {\r\n$code\r\n\t}";

            $updateCode  = <<<EOC
for( \$rowId = \$cursor->first(); !\$cursor->isDone(); \$rowId = \$cursor->next())
{
    \$updates = $updates;
    \$entry = \$updates + \$cursor->getRow();
$code
}
return true;
EOC;

            $success = eval($updateCode);
            if(!$success) {
                return $success;
            }

            $this->affected = $affected;
            if($this->affected)
            {
                if($this->auto)
                    $table->commit();
                else if(!in_array($table, $this->updatedTables))
                    $this->updatedTables[] = $table;
            }

            return true;
        } else {
            return $this->set_error('Invalid UPDATE query');
        }
    }

    private function where_key_check($rowId, $entry, &$key_lookup, $unique_columns)
    {
        foreach($unique_columns as $unique) {
            $current_lookup =& $key_lookup[$unique];
            $current_val = $entry[$unique];
            if(isset($current_lookup[$current_val])) {
                if($current_lookup[$current_val] != $rowId) {
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
        if(preg_match("/\AMERGE\s+INTO\s+(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(?:AS\s+)?`?([A-Z][A-Z0-9\_]*)`?)?\s+USING\s+(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s(?:AS\s+)?`?([^\W\d]\w*)`?)?\s+ON\s+(.+?)\s+(WHEN\s+(?:NOT\s+)?MATCHED.+?);?\Z/is", $query, $matches)) {
            list( , $dest_db_name, $dest_table_name, $dest_alias, $src_db_name, $src_table_name, $src_alias, $on_clause, $matches_clause) = $matches;

            if(!($dest_table = $this->find_table($dest_db_name, $dest_table_name))) {
                return false;
            }

            if(!($src_table = $this->find_table($src_db_name, $src_table_name))) {
                return false;
            }

            if(empty($dest_alias)) {
                $dest_alias = $dest_table_name;
            }

            if(empty($src_alias)) {
                $src_alias = $src_table_name;
            }

            $src_table_columns = $src_table->getColumns();
            $src_columns_size = count($src_table_columns);
            $joined_info = array(
                'tables' => array($src_alias => $src_table_columns),
                'offsets' => array($src_alias => 0),
                'columns' => array_keys($src_table_columns));

            $dest_table_columns = $dest_table->getColumns();
            $dest_table_column_names = array_keys($dest_table_columns);
            $dest_column_indices = array_flip($dest_table_column_names);

            $joined_info['tables'][$dest_alias] = $dest_table_columns;
            $new_offset = count($joined_info['columns']);
            $joined_info['columns'] = array_merge($joined_info['columns'], $dest_table_column_names);

            $conditional = $this->build_where($on_clause, $joined_info, FSQL_WHERE_ON);
            if(!$conditional) {
                return $this->set_error('Invalid ON clause: '.$this->error_msg);
            }

            if(!isset($this->join_lambdas[$conditional])) {
                $join_function = create_function('$left_entry,$right_entry', "return $conditional;");
                $this->join_lambdas[$conditional] = $join_function;
            } else {
                $join_function = $this->join_lambdas[$conditional];
            }

            $joined_info['offsets'][$dest_alias] = $new_offset;

            $hasMatched = false;
            $hasNotMatched = false;
            $matches_clause = trim($matches_clause);
            while(!empty($matches_clause)) {

                if(preg_match("/\AWHEN\s+MATCHED\s+(?:AND\s+(.+?)\s+)?THEN\s+UPDATE\s+SET\s+(.+?)(\s+when\s+.+|\s*\Z)/is", $matches_clause, $clause)) {
                    if($hasMatched) {
                        return $this->set_error("Can only have one WHEN MATCHED clause");
                    }
                    list(, $andClause, $setList, $matches_clause) = $clause;
                    $sets = $this->parseSetClause($setList, $dest_table_columns, $dest_alias);
                    if($sets === false) {
                        return false;
                    }

                    $updateCode = '';
                    foreach($sets as $set) {
                        $valueExpr = $this->build_expression($set[1], $joined_info, FSQL_WHERE_NORMAL);
                        if($valueExpr === false) {
                            return false;
                        }
                        $colIndex = $dest_column_indices[$set[0]];
                        $updateCode .= $colIndex . " => $valueExpr, ";
                    }
                    $updateCode = 'return array(' . substr($updateCode, 0, -2) . ');';

                    if(!empty($andClause)) {
                        $updateAndExpr = $this->build_where($andClause, $joined_info, FSQL_WHERE_NORMAL);
                        if(!$updateAndExpr) {
                            return $this->set_error('Invalid AND clause: '.$this->error_msg);
                        }
                        $updateCode = "if($updateAndExpr) { $updateCode } else { return false; }";
                    }
                    $hasMatched = true;
                } else if(preg_match("/\AWHEN\s+NOT\s+MATCHED\s+(?:AND\s+(.+?)\s+)?THEN\s+INSERT\s*(?:\((.+?)\))?\s*VALUES\s*\((.+?)\)(\s+when\s+.+|\s*\Z)/is", $matches_clause, $clause)) {
                    if($hasNotMatched) {
                        return $this->set_error("Can only have one WHEN NOT MATCHED clause");
                    }
                    list(, $andClause, $columnList, $values, $matches_clause) = $clause;
                    $columnList = trim($columnList);
                    if(!empty($columnList)) {
                        $columns = preg_split("/\s*,\s*/", columnList);
                    } else {
                        $columns = $dest_table_column_names;
                    }

                    preg_match_all("/\((?:[^()]|(?R))+\)|'[^']*'|[^(),\s]+/", $values, $valuesList);
                    $valuesList = $valuesList[0];

                    if(count($valuesList) != count($columns)) {
                        return $this->set_error("Number of inserted values and columns are not equal in MERGE WHEN NOT MATCHED clause");
                    }

                    $insertCode = '';
                    foreach($valuesList as $value) {
                        $valueExpr = $this->build_expression($value, $joined_info, FSQL_WHERE_NORMAL);
                        if($valueExpr === false) {
                            return false;
                        }
                        $insertCode .= $valueExpr.', ';
                    }
                    $insertCode = 'return array(' . substr($insertCode, 0, -2) . ');';

                    if(!empty($andClause)) {
                        $insertAndExpr = $this->build_where($andClause, $joined_info, FSQL_WHERE_NORMAL);
                        if(!$insertAndExpr) {
                            return $this->set_error('Invalid AND clause: '.$this->error_msg);
                        }
                        $insertCode = "if($insertAndExpr) { $insertCode } else { return false; }";
                    }

                    $hasNotMatched = true;
                } else {
                    return $this->set_error("Unknown MERGE WHEN clause");
                }

                $matches_clause = trim($matches_clause);
            }

            $joinMatches = array();
            $join_data = $this->left_join($src_table->getEntries(), $dest_table->getEntries(), $join_function, $src_columns_size, $joinMatches);

            $affected = 0;
            $srcCursor = $src_table->getCursor();
            $destCursor = $src_table->getCursor();
            for($srcRowId = $srcCursor->first(); !$srcCursor->isDone(); $srcRowId = $srcCursor->next()) {
                $entry = $srcCursor->getRow();
                $destRowId = $joinMatches[$srcRowId];
                if($destRowId === false) {
                    $newRow = eval($insertCode);
                    if($newRow !== false) {
                        $dest_table->insertRow($newRow);
                        ++$affected;
                    }
                } else {
                    $destCursor->seek($destRowId);
                    $destRow = $destCursor->getRow();
                    $entry = array_merge($entry, $destRow);
                    $updates = eval($updateCode);
                    if($updates !== false) {
                        $dest_table->updateRow($destRowId, $updates);
                        ++$affected;
                    }
                }
            }

            if($this->auto) {
                $dest_table->commit();
            } else if(!in_array($dest_table, $this->updatedTables)) {
                $this->updatedTables[] = $dest_table;
            }

            $this->affected = $affected;
            return true;
        } else {
            return $this->set_error("Invalid MERGE query");
        }
    }

    ////Select data from the DB
    private function query_select($query)
    {
        if(!preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*)\s*[;]?\s*\Z/is', $query, $matches)) {
            return $this->set_error('Invalid SELECT query');
        }

        $distinct = !strncasecmp($matches[1], "DISTINCT", 8);
        $has_random = strlen(trim($matches[2])) > 0;
        $isTableless = true;

        $oneAggregate = false;
        $selectedInfo = array();
        $the_rest = $matches[3];
        $stop = false;
        while(!$stop && preg_match("/((?:\A|\s*)(?:(-?\d+(?:\.\d+)?)|('.*?(?<!\\\\)')|(?:(`?([^\W\d]\w*)`?\s*\(.*?\)))|(?:(?:(?:`?([^\W\d]\w*)`?\.)?(`?([^\W\d]\w*)`?|\*))))(?:(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?))?\s*)(?:\Z|(from|where|having|(?:group|order)?\s+by|offset|fetch|limit)|,)/is", $the_rest, $colmatches))
        {
            $stop = !empty($colmatches[10]);
            $idx = !$stop ? 0 : 1;
            $the_rest = substr($the_rest, strlen($colmatches[$idx]));
            $alias = null;
            if(!empty($colmatches[2])) {  // int/float constant
                $value = $colmatches[2];
                $type = 'number';
                $alias = $value;
            } else if(!empty($colmatches[3])) {  // string constant
                $value = $colmatches[3];
                $type = 'string';
                $alias = $value;
            } else if(!empty($colmatches[4])) {  // function call
                $value = $colmatches[4];
                $function_name = strtolower($colmatches[5]);
                $type = 'function';
                list($alias, $function_type) = $this->lookup_function($function_name);
                if($function_type & FSQL_FUNC_AGGREGATE) {
                    $oneAggregate = true;
                }
            } else if(!empty($colmatches[7])) {  // column
                $column = $colmatches[7] !== '*' ? $colmatches[8] : $colmatches[7];
                $table_name = $colmatches[6];
                $value = !empty($table_name) ? $table_name.'.'.$column : $column;
                $alias = $column;
                $type = 'column';
            }
            if(!empty($colmatches[9])) {
                $alias = $colmatches[9];
                if(substr($value, -1) == '*') {
                    return $this->set_error("Can't not specify an alias for *");
                }
            }
            $selectedInfo[] = array($type, $value, $alias);
        }

        $data = array();
        $joins = array();
        $joined_info = array( 'tables' => array(), 'offsets' => array(), 'columns' =>array() );
        if(preg_match('/\Afrom\s+(.+?)(\s+(?:where|having|(:group|order)?\s+by|offset|fetch|limit)\s+(?:.+))?\s*\Z/is', $the_rest, $from_matches))
        {
            $isTableless = false;
            $tables = array();

            if(isset($from_matches[2])) {
                $the_rest = $from_matches[2];
            }

            $tbls = explode(',', $from_matches[1]);
            foreach($tbls as $tbl) {
                if(preg_match('/\A\s*(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s*(.*)/is', $tbl, $table_matches)) {
                    list(, $db_name, $table_name, $saveas, $table_unparsed) = $table_matches;
                    if(empty($saveas)) {
                        $saveas = $table_name;
                    }

                    if(!($table = $this->find_table($db_name, $table_name))) {
                        return false;
                    }

                    if(!isset($tables[$saveas])) {
                        $tables[$saveas] = $table;
                    } else {
                        return $this->set_error("Table named '$saveas' already specified");
                    }

                    $joins[$saveas] = array('fullName' => array($db_name, $table_name), 'joined' => array());
                    $table_columns = $table->getColumns();
                    $join_columns_size = count($table_columns);
                    $joined_info['tables'][$saveas] = $table_columns;
                    $joined_info['offsets'][$saveas] = count($joined_info['columns']);
                    $joined_info['columns'] = array_merge($joined_info['columns'], array_keys($table_columns));

                    $join_data = $table->getEntries();

                    if(!empty($table_unparsed)) {
                        preg_match_all("/\s*(?:((?:LEFT|RIGHT|FULL)(?:\s+OUTER)?|INNER)\s+)?JOIN\s+(`?(?:[^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(?:AS\s+)?`?([^\W\d]\w*)`?)?\s+(USING|ON)\s*(?:(?:\((.*?)\))|(?:(?:\()?((?:\S+)\s*=\s*(?:\S+)(?:\))?)))/is", $table_unparsed, $join);
                        $numJoins = count($join[0]);
                        for($i = 0; $i < $numJoins; ++$i) {
                            $join_name = trim($join[1][$i]);
                            $join_db_name = $join[2][$i];
                            $join_table_name = $join[3][$i];
                            $join_table_saveas = $join[4][$i];
                            if(empty($join_table_saveas)) {
                                $join_table_saveas = $join_table_name;
                            }

                            if(!($join_table = $this->find_table($join_db_name, $join_table_name))) {
                                return false;
                            }

                            if(!isset($tables[$join_table_saveas])) {
                                $tables[$join_table_saveas] = $join_table;
                            } else {
                                return $this->set_error("Table named '$join_table_saveas' already specified");
                            }

                            $clause = $join[5][$i];
                            if(!strcasecmp($clause, "ON")) {
                                $conditions = isset($list[6][$i]) ? $join[6][$i] : $join[7][$i];
                            }
                            else if(!strcasecmp($clause, "USING")) {
                                $shared_columns = preg_split('/\s*,\s*/', trim($join[6][$i]));

                                $conditional = '';
                                foreach($shared_columns as $shared_column) {
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

                            $conditional = $this->build_where($conditions, $joined_info, FSQL_WHERE_ON);
                            if(!$conditional) {
                                return $this->set_error('Invalid ON/USING clause: '.$this->error_msg);
                            }

                            if(!isset($this->join_lambdas[$conditional])) {
                                $join_function = create_function('$left_entry,$right_entry', "return $conditional;");
                                $this->join_lambdas[$conditional] = $join_function;
                            } else {
                                $join_function = $this->join_lambdas[$conditional];
                            }

                            $joined_info['offsets'][$join_table_saveas] = $new_offset;
                            $joins[$saveas]['joined'][] = array('alias' => $join_table_saveas, 'fullName' => array($join_db_name, $join_table_name), 'type' => $join_name, 'clause' => $clause, 'comparator' => $join_function);

                            $joining_entries = $join_table->getEntries();
                            if(!strncasecmp($join_name, "LEFT", 4)) {
                                $joinMatches = array();
                                $join_data = $this->left_join($join_data, $joining_entries, $join_function, $joining_columns_size, $joinMatches);
                                unset($joinMatches);
                            } else if(!strncasecmp($join_name, "RIGHT", 5)) {
                                $join_data = $this->right_join($join_data, $joining_entries, $join_function, $join_columns_size);
                            } else if(!strncasecmp($join_name, "FULL", 4)) {
                                $join_data = $this->full_join($join_data, $joining_entries, $join_function, $join_columns_size, $joining_columns_size);
                            } else {
                                $join_data = $this->inner_join($join_data, $joining_entries, $join_function);
                            }

                            $join_columns_size += $joining_columns_size;
                        }
                    }

                    // implicit CROSS JOINs
                    if(!empty($join_data)) {
                        if(!empty($data)) {
                            $new_data = array();
                            foreach($data as $left_entry)
                            {
                                foreach($join_data as $right_entry) {
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

        $this->tosort = array();
        $where = null;
        $group_key = null;
        $isGrouping = false;
        $having = null;
        $limit = null;
        $singleRow = false;

        if(preg_match('/\s+WHERE\s+((?:.+?)(?:(?:(?:(?:\s+)(?:AND|OR)(?:\s+))?(?:.+?)?)*?)?)(\s+(?:HAVING|(?:GROUP|ORDER)\s+BY|LIMIT|OFFET|FETCH).*)?\Z/is', $the_rest, $additional)) {
            $the_rest = isset($additional[2]) ? $additional[2] : '';
            $where = $this->build_where($additional[1], $joined_info);
            if(!$where)
                return $this->set_error('Invalid WHERE clause: ' . $this->error_msg);
        }

        if(preg_match('/\s+GROUP\s+BY\s+(.*?)(\s+(?:HAVING|ORDER\s+BY|OFFSET|FETCH|LIMIT).*)?\Z/is', $the_rest, $additional)) {
            $the_rest = isset($additional[2]) ? $additional[2] : '';
            $GROUPBY = explode(',', $additional[1]);
            $joined_info['group_columns'] = array();
            $isGrouping = true;
            $group_array = array();
            $group_key_list = '';
            foreach($GROUPBY as $group_item)
            {
                if(preg_match('/(?:`?([^\W\d]\w*)`?\.)?`?`?([^\W\d]\w*)`?/is', $group_item, $additional)) {
                    list( , $table_alias, $column) = $additional;
                    $group_col = $this->find_column($column, $table_alias, $joined_info, "GROUP BY clause");
                    if($group_col === false) {
                        return false;
                    }

                    $group_array[] = $group_col;
                    $group_key_list .= '$entry[' . $group_col .'], ';
                    $joined_info['group_columns'][] = $group_col;
                }
            }

            $group_key = substr($group_key_list, 0, -2);
            if(count($group_array) > 1) {
                $group_key = 'serialize(array('. $group_key . '))';
            }
        }

        if(preg_match('/\s+HAVING\s+((?:.+?)(?:(?:(?:(?:\s+)(?:AND|OR)(?:\s+))?(?:.+?)?)*?)?)(?:\s+(?:ORDER\s+BY|OFFSET|FETCH|LIMIT).*)?\Z/is', $the_rest, $additional)) {
            $the_rest = isset($additional[2]) ? $additional[2] : '';
            if(!isset($joined_info['group_columns'])) { // no GROUP BY
                $joined_info['group_columns'] = array();
                $isGrouping = true;
                $singleRow = true;
            }

            $having = $this->build_where($additional[1], $joined_info, FSQL_WHERE_HAVING);
            if(!$having) {
                return $this->set_error('Invalid HAVING clause: ' . $this->error_msg);
            }
        }

        if(preg_match('/\s+ORDER\s+BY\s+(.*?)(\s+(?:OFFSET|FETCH|LIMIT).*)?\Z/is', $the_rest, $additional)) {
            $the_rest = isset($additional[2]) ? $additional[2] : '';
            $ORDERBY = explode(',', $additional[1]);
            foreach($ORDERBY as $order_item) {
                if(preg_match('/(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+(ASC|DESC))?/is', $order_item, $additional)) {
                    list( , $table_alias, $column) = $additional;
                    $index = $this->find_column($column, $table_alias, $joined_info, "ORDER BY clause");
                    if($index === false) {
                        return false;
                    }

                    $ascend = !empty($additional[3]) ? !strcasecmp('ASC', $additional[3]) : true;
                    $this->tosort[] = array('key' => $index, 'ascend' => $ascend);
                }
            }
        }

        $limit_start = null;
        if(preg_match('/\s+OFFSET\s+(\d+)\s+ROWS?\b/is', $the_rest, $additional)) {
            $limit_start = (int) $additional[1];
        }

        if(preg_match('/\s+FETCH\s+(?:FIRST|NEXT)\s+(?:(\d+)\s+)?ROWS?\s+ONLY\b/is', $the_rest, $additional)) {
            $limit_stop = isset($additional[1]) ? (int) $additional[1] : 1;
            if($limit_start === null)
                $limit_start = 0;
            $limit = array($limit_start, $limit_stop);
        }
        else if($limit_start !== null) {
            // OFFSET without FETCH FIRST
            $limit = array($limit_start, NULL);
        }

        if(preg_match('/\s+LIMIT\s+(?:(?:(\d+)\s*,\s*(\-1|\d+))|(?:(\d+)\s+OFFSET\s+(\d+))|(\d+))/is', $the_rest, $additional)) {
            if($limit === null) {
                if(isset($additional[5])) {
                    // LIMIT length
                    $limit_stop = $additional[5]; $limit_start = 0;
                } else if(isset($additional[3])) {
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

        if(!$isGrouping && $oneAggregate)
            $isGrouping = true;

        $selected_columns = array();
        $final_code = null;
        if($isGrouping)
        {
            $select_line = '';
            foreach($selectedInfo as $info) {
                list($select_type, $select_value, $select_alias) = $info;
                $column_info = null;
                switch($select_type) {
                    case 'column':
                        if(strpos($select_value, '.') !== false) {
                            list($table_name, $column) = explode('.', $select_value);
                        } else {
                            $table_name = null;
                            $column = $select_value;
                        }

                        if(!strcasecmp($select_value, 'NULL')) {
                            $select_line .= 'NULL, ';
                            $selected_columns[] = $select_alias;
                            continue;
                        } else {
                            $index = $this->find_column($column, $table_name, $joined_info, 'SELECT clause');
                            if($index === false) {
                                return false;
                            }
                            $select_line .= "\$this->trim_quotes(\$entry[$index]), ";
                            $selected_columns[] = $select_alias;
                        }

                        if(!in_array($index, $group_array)) {
                            return $this->set_error("Selected column '{$joined_info['columns'][$index]}' is not a grouped column");
                        }
                        $select_line .= "\$this->trim_quotes(\$group[0][$index]), ";
                        $selected_columns[] = $select_alias;
                        break;
                    case 'number':
                    case 'string':
                        $select_line .= $select_value.', ';
                        $selected_columns[] = $select_alias;
                        break;
                    case 'function':
                        $expr = $this->build_expression($select_value, $joined_info);
                        if($expr === false) {
                            return false;
                        }
                        $select_line .= $expr.', ';
                        $selected_columns[] = $select_alias;
                        break;
                }
                $column_info['name'] = $select_alias;
                $fullColumnsInfo[] = $column_info;
            }

            if(!$singleRow)
                $line = '$grouped_set['.$group_key.'][] = $entry;';
            else
                $line = '$group[] = $entry;';

            $final_line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
            $grouped_set = array();

            if($having !== null) {
                $final_line = "if({$having}) {\r\n\t\t\t\t\t\t$final_line\r\n\t\t\t\t\t}";
            }

            if(!$singleRow) {
                $final_code = <<<EOT
                foreach(\$grouped_set as \$group) {
                    $final_line
                }
EOT;
            } else {
                $final_code = $final_line;
            }
        }
        else
        {
            $select_line = '';
            foreach($selectedInfo as $info) {
                list($select_type, $select_value, $select_alias) = $info;
                switch($select_type) {
                // function call
                case 'function':
                    $expr = $this->build_expression($select_value, $joined_info, false);
                    if($expr !== false)    {
                        $select_line .= $expr.', ';
                        $selected_columns[] = $select_alias;
                    } else {
                        return false; // error should already be set by parser
                    }
                    break;

                case 'column':
                    if(strpos($select_value, '.') !== false) {
                        list($table_name, $column) = explode('.', $select_value);
                    } else {
                        $table_name = null;
                        $column = $select_value;
                    }

                    if($column === '*') {
                        $star_tables = !empty($table_name) ? array($table_name) : array_keys($tables);
                        foreach($star_tables as $tname) {
                            $start_index = $joined_info['offsets'][$tname];
                            $table_columns = $joined_info['tables'][$tname];
                            $column_names = array_keys($table_columns);
                            foreach($column_names as $index => $column_name) {
                                $select_value = $start_index + $index;
                                $select_line .= "\$this->trim_quotes(\$entry[$select_value]), ";
                                $selected_columns[] = $column_name;
                            }
                        }

                        continue;
                    } else if(!strcasecmp($select_value, 'NULL')) {
                        $select_line .= 'NULL, ';
                        $selected_columns[] = $select_alias;
                        continue;
                    } else {
                        $index = $this->find_column($column, $table_name, $joined_info, 'SELECT clause');
                        if($index === false) {
                            return false;
                        }
                        $select_line .= "\$this->trim_quotes(\$entry[$index]), ";
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

            $line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
            $group = $data;
        }

        if(!empty($joins)) {
            if($where !== null)
                $line = "if({$where}) {\r\n\t\t\t\t\t$line\r\n\t\t\t\t}";

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

        if(!empty($this->tosort)) {
            usort($final_set, array($this, "orderBy"));
        }

        if($limit !== null) {
            $stop = $limit[1];
            if($stop !== NULL) {
                $final_set = array_slice($final_set, $limit[0], $stop);
            } else {
                $final_set = array_slice($final_set, $limit[0]);
            }
        }

        if(!empty($final_set) && $has_random && preg_match("/\s+RANDOM(?:\((\d+)\)?)?\s+/is", $select, $additional)) {
            $results = array();
            if(!$additional[1]) { $additional[1] = 1; }
            if($additional[1] <= count($this_random)) {
                $random = array_rand($final_set, $additional[1]);
                if(is_array($random)) {    foreach($random as $key) { $results[] = $final_set[$key]; }    }
                else { $results[] = $final_set[$random]; }
            }
            unset($final_set);
            $final_set = $results;
        }

        return $this->create_result_set($selected_columns, $final_set);
    }

    private function trim_quotes($string)
    {
        return preg_replace("/^'(.+)'$/s", "\\1", $string);
    }

    private function find_column($column, $table_name, $joined_info, $where) {
        if(!empty($table_name)) {
            if(!isset($joined_info['tables'][$table_name])) {
                return $this->set_error("Unknown table name/alias in $where: $table_name");
            }

            $index = array_search($column,  array_keys($joined_info['tables'][$table_name])) + $joined_info['offsets'][$table_name];
            if($index === false || $index === null) {
                return $this->set_error("Unknown column in $where: $column");
            }
        } else {
            $index = $this->find_exactly_one($joined_info, $column, $where);
            if($index === false) {
                return false;
            }
        }

        return $index;
    }

    private function parseSetClause($clause, $columns, $tableAlias = null) {
        $result = array();
        if(preg_match_all("/((?:\S+)\s*=\s*(?:'(?:.*?)'|\S+))`?\s*(?:,|\Z)/is", $clause, $sets)) {
            foreach($sets[1] as $set) {
                $s = preg_split("/`?\s*=\s*`?/", $set);
                if(preg_match("/\A\s*(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?/is", $s[0], $namePieces)) {
                    list(, $prefix, $columnName) = $namePieces;
                    if($tableAlias !== null && !empty($prefix) && $tableAlias !== $prefix) {
                        return $this->set_error("Unknown table alias in SET clause");
                    }
                    $s[0] = $columnName;
                }

                $result[] = $s;
                if(!isset($columns[$s[0]])) {
                    return $this->set_error("Invalid column name '{$s[0]}' found in SET clause");
                }
            }
        } else {
            $result[0] = preg_split("/\s*=\s*/", $clause);
        }
        return $result;
    }

    private function build_expression($exprStr, $join_info, $where_type = FSQL_WHERE_NORMAL)
    {
        $expr = false;

        // function call
        if(preg_match('/\A([^\W\d]\w*)\s*\((.*?)\)/is', $exprStr, $matches)) {
            $function = strtolower($matches[1]);
            $params = $matches[2];
            $final_param_list = '';
            $paramExprs = array();
            $expr_type = '"non-constant"';

            $functionInfo = $this->lookup_function($function);
            if($functionInfo === false) {
                return false;
            }

            list($function, $type) = $functionInfo;
            $isAggregate = $type & FSQL_FUNC_AGGREGATE;

            if($isAggregate) {
                $paramExprs[] = '$group';
            }

            if($type & FSQL_FUNC_CUSTOM_PARSE)    {
                $originalFunction = $function;
                $parseFunction = "parse_{$function}_function";
                $parsedData = $this->$parseFunction($join_info, $where_type | 1, $params);
                if($parsedData !== false) {
                    list($function, $paramExprs, ) = $parsedData;
                    if(isset($parsedData[2]))  // used by CAST() to override based on params
                        $columnData['type'] = $parsedData[2];
                } else {
                    return $this->set_error("Error parsing parameters for function $originalFunction");
                }
            }
            else if(strlen($params) !== 0) {
                $parameter = explode(',', $params);
                foreach($parameter as $param) {
                    $param = trim($param);


                    if($isAggregate && $param === '*') {
                        if($function === 'count') {
                            $paramExprs[] = '"*"';
                        } else {
                            return $this->set_error('Passing * as a paramter is only allowed in COUNT');
                        }
                    } else {
                        $paramExpr = $this->build_expression($param, $join_info, $where_type | 1);
                        if($paramExpr === false) // parse error
                            return false;

                        if($isAggregate)
                        {
                            if(preg_match('/\\$entry\[(\d+)\]/', $paramExpr, $paramExpr_matches))
                                $paramExprs[] = $paramExpr_matches[1];
                            else //assume everything else is some form of constant
                            {
                                $expr_type = '"constant"';
                                $paramExprs[] = $pexp;
                            }
                        } else {
                            $paramExprs[] = $paramExpr;
                        }
                    }
                }
            }

            if($isAggregate)
                $paramExprs[] = $expr_type;

            $final_param_list = implode(',', $paramExprs);

            if($type != FSQL_FUNC_REGISTERED)
                $expr = "\$this->functions->$function($final_param_list)";
            else
                $expr = "$function($final_param_list)";
        }
        // column/alias/keyword
        else if(preg_match('/\A(?:`?([^\W\d]\w*|\{\{left\}\})`?\.)?`?([^\W\d]\w*)`?\Z/is', $exprStr, $matches)) {
            list( , $table_name, $column) =  $matches;
            // table.column
            if($table_name) {
                if(isset($join_info['tables'][$table_name])) {
                    $table_columns = $join_info['tables'][$table_name];
                    if(isset($table_columns[ $column ])) {
                        if( isset($join_info['offsets'][$table_name]) ) {
                            $colIndex = array_search($column,  array_keys($table_columns)) + $join_info['offsets'][$table_name];
                            if($where_type === FSQL_WHERE_HAVING) { // column/alias in grouping clause
                                if(in_array($colIndex, $join_info['group_columns'])) {
                                    $expr = "\$group[0][$colIndex]";
                                } else {
                                    return $this->set_error("Column $column is not a grouped column");
                                }
                            } else {
                                $expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
                            }
                        } else {
                            $colIndex = array_search($column, array_keys($table_columns));
                            $expr = "\$right_entry[$colIndex]";
                        }
                    } else {
                        return $this->set_error("Unknown column $column for table $table_name");
                    }
                } else if($where_type & FSQL_WHERE_ON && $table_name === '{{left}}') {
                    $colIndex = $this->find_exactly_one($joined_info, $column, "expression");
                    if($colIndex === false) {
                        return false;
                    }
                    $expr = "\$left_entry[$colIndex]";
                } else {
                    return $this->set_error('Unknown table/alias '.$table_name);
                }

            }
            // null/unkown
            else if(!strcasecmp($exprStr, 'NULL') || !strcasecmp($exprStr, 'UNKNOWN')) {
                $expr = 'NULL';
            }
            // true/false
            else if(!strcasecmp($exprStr, 'TRUE') || !strcasecmp($exprStr, 'FALSE')) {
                $expr = strtoupper($exprStr);
            } else {  // column/alias no table
                $colIndex = $this->find_exactly_one($join_info, $column, "expression");
                if($colIndex === false) {
                    return false;
                }
                if($where_type === FSQL_WHERE_HAVING) { // column/alias in grouping clause
                    if(in_array($colIndex, $join_info['group_columns'])) {
                        $expr = "\$group[0][$colIndex]";
                    } else {
                        return $this->set_error("Column $column is not a grouped column");
                    }
                } else {
                    $expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
                }
            }
        }
        // number
        else if(preg_match('/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is', $exprStr)) {
            $expr = $exprStr;
        }
        // string
        else if(preg_match("/\A'.*?(?<!\\\\)'\Z/is", $exprStr)) {
            $expr = $exprStr;
        }
        else if(($where_type & FSQL_WHERE_ON) && preg_match('/\A{{left}}\.`?([^\W\d]\w*)`?/is', $exprStr, $matches)) {
            $colIndex = $this->find_exactly_one($join_info, $column, "expression");
            if($colIndex === false) {
                return false;
            }

            $expr = "\$left_entry[$colIndex]";
        }
        else
            return false;

        return $expr;
    }

    private function find_exactly_one($join_info, $column, $location) {
        $keys = array_keys($join_info['columns'], $column);
        $keyCount = count($keys);
        if($keyCount == 0) {
            return $this->set_error("Unknown column/alias in $location: $column");
        } else if($keyCount > 1) {
            return $this->set_error("Ambiguous column/alias in $location: $column");
        }
        return $keys[0];
    }

    private function build_where($statement, $join_info, $where_type = FSQL_WHERE_NORMAL)
    {
        if($statement) {
            preg_match_all("/(\A\s*|\s+(?:AND|OR)\s+)(NOT\s+)?(\S+?)(\s*(?:!=|<>|>=|<=>?|>|<|=)\s*|\s+(?:IS(?:\s+NOT)?|(?:NOT\s+)?IN|(?:NOT\s+)?R?LIKE|(?:NOT\s+)?REGEXP)\s+)(\((.*?)\)|'.*?'|\S+)/is", $statement, $WHERE, PREG_SET_ORDER);

            if(empty($WHERE))
                return false;

            $condition = '';
            foreach($WHERE as $where)
            {
                $local_condition = '';
                $logicalOp = trim($where[1]);
                $not = !empty($where[2]);
                $leftStr = $where[3];
                $operator = preg_replace('/\s+/', ' ', trim(strtoupper($where[4])));
                $rightStr = $where[5];

                $leftExpr = $this->build_expression($leftStr, $join_info, $where_type);
                if($leftExpr === false)
                    return false;

                if($operator !== 'IN' && $operator !== 'NOT IN')
                {
                    $rightExpr = $this->build_expression($rightStr, $join_info, $where_type);
                    if($rightExpr === false)
                        return false;

                    switch($operator) {
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
                            if($rightExpr === 'NULL')
                                $local_condition = "($leftExpr === null ? FSQL_TRUE : FSQL_FALSE)";
                            else if($rightExpr === 'TRUE')
                                $local_condition = "\$this->functions->isTrue($leftExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            else if($rightExpr === 'FALSE')
                                $local_condition = "\$this->functions->isFalse($leftExpr) ? FSQL_TRUE : FSQL_FALSE)";
                            else
                                return false;
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
                }
                else
                {
                    if(!empty($where[6])) {
                        $array_values = explode(',', $where[6]);
                        $valuesExpressions = array();
                        foreach($array_values as $value)
                        {
                            $valueExpr = $this->build_expression(trim($value), $join_info, $where_type);
                            $valuesExpressions[] = $valueExpr['expression'];
                        }
                        $valuesString = implode(',', $valuesExpressions);
                        $local_condition = "\$this->functions->in($leftExpr, array($valuesString))";

                        if($operator === 'NOT IN')
                            $not = !$not;
                    }
                    else
                        return false;
                }

                if(!strcasecmp($logicalOp, 'AND'))
                    $condition .= ' & ';
                else if(!strcasecmp($logicalOp, 'OR'))
                    $condition .= ' | ';

                if($not)
                    $condition .= '\$this->functions->not('.$local_condition.')';
                else
                    $condition .= $local_condition;
            }
            return "($condition) === ".FSQL_TRUE;
        }
        return false;
    }

    private function orderBy($a, $b)
    {
        foreach($this->tosort as $tosort) {
            extract($tosort);
            $a[$key] = preg_replace("/^'(.+?)'$/", "\\1", $a[$key]);
            $b[$key] = preg_replace("/^'(.+?)'$/", "\\1", $b[$key]);
            if (($a[$key] > $b[$key] && $ascend) || ($a[$key] < $b[$key] && !$ascend)) {
                return 1;
            } else if (($a[$key] < $b[$key] && $ascend) || ($a[$key] > $b[$key] && !$ascend)) {
                return -1;
            }
        }
    }

    ////Delete data from the DB
    private function query_delete($query)
    {
        $this->affected  = 0;
        if(preg_match('/\ADELETE\s+FROM\s+(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?(?:\s+WHERE\s+(.+?))?\s*[;]?\Z/is', $query, $matches)) {
            list(, $db_name, $table_name) = $matches;

            $table = $this->find_table($db_name, $table_name);
            if($table === false) {
                return false;
            } else if($table->isReadLocked()) {
                return $this->error_table_read_lock($table->fullName());
            }

            $columns = $table->getColumns();
            $columnNames = array_keys($columns);
            $cursor = $table->getCursor();

            if($cursor->isDone())
                return true;


            if(isset($matches[3])) {
                $where = $this->build_where($matches[3], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
                if(!$where) {
                    return $this->set_error('Invalid WHERE clause: '.$this->error_msg);
                }

                $code = <<<EOC
            for(\$k = \$cursor->first(); !\$cursor->isDone(); \$k = \$cursor->next()) {
                \$entry = \$cursor->getRow();
                if({$where})
                {
                    \$table->deleteRow(\$k);
                    \$this->affected++;
                }
            }
EOC;

                eval($code);
            } else {
                $c = 0;
                for($k = $cursor->first(); !$cursor->isDone(); $k = $cursor->next()) {
                    $table->deleteRow($k);
                    ++$c;
                }
                $this->affected = $c;
            }

            if($this->affected)
            {
                if($this->auto)
                    $table->commit();
                else if(!in_array($table, $this->updatedTables))
                    $this->updatedTables[] = $table;
            }

            return true;
        } else {
            return $this->set_error("Invalid DELETE query");
        }
    }

    private function query_alter($query)
    {
        if(preg_match("/\AALTER\s+(TABLE|SEQUENCE)\s+/is", $query, $matches)) {
            if(!strcasecmp($matches[1], 'TABLE')) {
                return $this->query_alter_table($query);
            } else {
                return $this->query_alter_sequence($query);
            }
        } else {
            return $this->set_error('Invalid ALTER query');
        }
    }

    private function query_alter_sequence($query)
    {
        if(preg_match("/\AALTER\s+SEQUENCE\s+(?:(IF\s+EXISTS)\s+)?(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?\s+(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $ifExists, $dbName, $sequenceName, $valuesList) = $matches;
            $db = $this->get_database($dbName);
            if($db === false) {
                return false;
            }

            $sequences = $db->getSequences();
            if(!$sequences->exists()) {
                $sequences->create();
            }

            $sequence = $sequences->getSequence($sequenceName);
            if($sequence === false) {
                if(empty($ifExists)) {
                    return $this->set_error("Sequence {$db->name()}.{$sequenceName} does not exist");
                } else {
                    return true;
                }
            }

            $parsed = $this->parse_sequence_options($valuesList, true);
            if($parsed === false) {
                return false;
            }

            $result = $sequence->alter($parsed);
            if($result !== true) {
                $sequence->load();  // refresh temp changes made
                return $this->set_error($result);
            }

            return true;
        } else {
            return $this->set_error('Invalid ALTER SEQUENCE query');
        }
    }

    private function query_alter_table($query)
    {
        if(preg_match("/\AALTER\s+TABLE\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s+(.*)/is", $query, $matches)) {
            list(, $db_name, $table_name, $changes) = $matches;

            $tableObj = $this->find_table($db_name, $table_name);
            if($tableObj === false) {
                return false;
            } else if($tableObj->isReadLocked()) {
                return $this->error_table_read_lock($tableObj->fullName());
            }

            $columns =  $tableObj->getColumns();

            preg_match_all("/(?:ADD|ALTER|DROP|RENAME).*?(?:,|\Z)/is", trim($changes), $specs);
            for($i = 0; $i < count($specs[0]); $i++) {
                if(preg_match("/\AADD\s+(?:CONSTRAINT\s+`?[A-Z][A-Z0-9\_]*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/is", $specs[0][$i], $matches)) {
                    $columnDef =& $columns[$matches[1]];

                    foreach($columns as $name => $column) {
                        if($column['key'] == 'p') {
                            return $this->set_error("Primary key already exists");
                        }
                    }

                    $columnDef['key'] = 'p';
                    $tableObj->setColumns($columns);

                    return true;
                } else if(preg_match("/\AALTER(?:\s+(?:COLUMN))?\s+`?([A-Z][A-Z0-9\_]*)`?\s+(.+?)(?:,|;|\Z)/is", $specs[0][$i], $matches)) {
                    list(, $columnName, $the_rest) = $matches;
                    if(!isset($columns[$columnName])) {
                        return $this->set_error("Column named '$columnName' does not exist in table '$table_name'");
                    }

                    $columnDef =& $columns[$columnName];
                    if(preg_match("/(?:SET\s+DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')|DROP\s+DEFAULT)/is", $the_rest, $defaults)) {
                        if(!empty($defaults[1])) {
                            $default = $this->parseDefault($defaults[1], $columnDef['type'], $columnDef['null'], $columnDef['restraint']);
                        } else {
                            if($columnDef['null']) {
                                $default = "NULL";
                            } else {
                                $type = $columnDef['type'];
                                if($type === FSQL_TYPE_STRING) {
                                    $default = '';
                                } else if($type === FSQL_TYPE_FLOAT) {
                                    $default = 0.0;
                                } else {
                                    $default = 0;
                                }
                            }
                        }

                        $columnDef['default'] = $default;
                        $tableObj->setColumns($columns);
                    } else if(preg_match("/\ADROP\s+IDENTITY/i", $the_rest, $defaults)) {
                        if(!$columnDef['auto']) {
                            return $this->set_error("Column $columnName is not an identity column");
                        }
                        $tableObj->dropIdentity();
                    } else {
                        $parsed = $this->parse_sequence_options($the_rest, true);
                        if($parsed === false) {
                            return false;
                        } else if(!empty($parsed)) {
                            if(!$columnDef['auto']) {
                                return $this->set_error("Column $columnName is not an identity column");
                            }

                            $identity = $tableObj->getIdentity();
                            $result = $identity->alter($parsed);
                            if($result !== true) {
                                $identity->load();  // refresh temp changes made
                                return $this->set_error($result);
                            }
                        }
                    }

                    return true;
                } else if(preg_match("/\ADROP\s+PRIMARY\s+KEY/is", $specs[0][$i], $matches)) {
                    $found = false;
                    foreach($columns as $name => $column) {
                        if($column['key'] == 'p') {
                            $columns[$name]['key'] = 'n';
                            $found = true;
                        }
                    }

                    if($found) {
                        $tableObj->setColumns($columns);
                        return true;
                    } else {
                        return $this->set_error("No primary key found");
                    }
                }
                else if(preg_match("/\ARENAME\s+(?:TO\s+)?`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $specs[0][$i], $matches)) {
                    list(, $new_db_name, $new_table_name) = $matches;

                    $db = $tableObj->database();

                    $new_db = $this->get_database($new_db_name);
                    if($new_db === false) {
                        return false;
                    }

                    $new_table = $new_db->getTable($new_table_name);
                    if($new_table->exists()) {
                        return $this->set_error("Destination table {$new_db_name}.{$new_table_name} already exists");
                    }

                    return $db->renameTable($table_name, $new_table_name, $new_db);
                }
                else {
                    return $this->set_error("Invalid ALTER TABLE query");
                }
            }
        } else {
            return $this->set_error("Invalid ALTER TABLE query");
        }
    }

    private function parseDefault($default, $type, $null, $restraint)
    {
        if(strcasecmp($default, "NULL")) {
            if(preg_match("/\A'(.*)'\Z/is", $default, $matches)) {
                if($type == FSQL_TYPE_INTEGER)
                    $default = (int) $matches[1];
                else if($type == FSQL_TYPE_FLOAT)
                    $default = (float) $matches[1];
                else if($type == FSQL_TYPE_ENUM) {
                    if(in_array($default, $restraint))
                        $default = array_search($default, $restraint) + 1;
                    else
                        $default = 0;
                } else if($type == FSQL_TYPE_STRING) {
                    $default = $matches[1];
                }
            } else {
                if($type == FSQL_TYPE_INTEGER)
                    $default = (int) $default;
                else if($type == FSQL_TYPE_FLOAT)
                    $default = (float) $default;
                else if($type == FSQL_TYPE_ENUM) {
                    $default = (int) $default;
                    if($default < 0 || $default > count($restraint)) {
                        return $this->set_error("Numeric ENUM value out of bounds");
                    }
                } else if($type == FSQL_TYPE_STRING) {
                    $default = "'".$matches[1]."'";
                }
            }
        } else if(!$null) {
            if($type === FSQL_TYPE_STRING) {
                $default = '';
            } else if($type === FSQL_TYPE_FLOAT) {
                $default = 0.0;
            } else {
                // The default for dates, times, enums, and int types is 0
                $default = 0;
            }
        }

        return $default;
    }

    private function query_rename($query)
    {
        if(preg_match("/\ARENAME\s+TABLE\s+(.*)\s*[;]?\Z/is", $query, $matches)) {
            $tables = explode(",", $matches[1]);
            foreach($tables as $table) {
                list($old, $new) = preg_split("/\s+TO\s+/i", trim($table));

                if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $old, $table_parts)) {
                    list(, $old_db_name, $old_table_name) = $table_parts;

                    $old_db = $this->get_database($old_db_name);
                    if($old_db === false) {
                        return false;
                    }
                } else {
                    return $this->set_error("Parse error in table listing");
                }

                if(preg_match("/(?:([A-Z][A-Z0-9\_]*)\.)?([A-Z][A-Z0-9\_]*)/is", $new, $table_parts)) {
                    list(, $new_db_name, $new_table_name) = $table_parts;

                    $new_db = $this->get_database($new_db_name);
                    if($new_db === false) {
                        return false;
                    }
                } else {
                    return $this->set_error("Parse error in table listing");
                }

                $old_table = $this->find_table($old_db_name, $old_table_name);
                if($old_table === false) {
                    return false;
                }
                elseif($old_table->isReadLocked()) {
                    return $this->error_table_read_lock($old_table_name->fullName());
                }

                $new_table = $new_db->getTable($new_table_name);
                if($new_table->exists()) {
                    return $this->set_error("Destination table {$new_db_name}.{$new_table_name} already exists");
                }

                return $old_db->renameTable($old_table_name, $new_table_name, $new_db);
            }
            return true;
        } else {
            return $this->set_error("Invalid RENAME query");
        }
    }

    private function query_drop($query)
    {
        if(preg_match("/\ADROP(?:\s+(TEMPORARY))?\s+TABLE(?:\s+(IF\s+EXISTS))?\s+(.*)\s*[;]?\Z/is", $query, $matches)) {
            $temporary = !empty($matches[1]);
            $ifexists = !empty($matches[2]);
            $tables = explode(",", $matches[3]);

            foreach($tables as $table) {
                if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_parts)) {
                    list(, $db_name, $table_name) = $table_parts;

                    $db = $this->get_database($db_name);
                    if($db === false) {
                        return false;
                    }

                    $table = $db->getTable($table_name);
                    if($table->isReadLocked()) {
                        return $this->error_table_read_lock($db->name, $table_name);
                    }

                    $existed = $db->dropTable($table_name);
                    if(!$ifexists && !$existed) {
                        return $this->error_table_not_exists($db->name, $table_name);
                    }
                } else {
                    return $this->set_error("Parse error in table listing");
                }
            }
            return true;
        } else if(preg_match("/\ADROP\s+DATABASE(?:\s+(IF\s+EXISTS))?\s+`?([A-Z][A-Z0-9\_]*)`?\s*[;]?\Z/is", $query, $matches)) {
            $ifexists = !empty($matches[1]);
            $db_name = $matches[2];

            if(!isset($this->databases[$db_name])) {
                if(!$ifexists) {
                    return $this->set_error("Database '{$db_name}' does not exist");
                } else {
                    return true;
                }
            }

            $db = $this->databases[$db_name];
            $db->drop();
            unset($this->databases[$db_name]);

            return true;
        } else if(preg_match("/\ADROP\s+SEQUENCE(?:\s+(IF\s+EXISTS))?\s+(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?\s*[;]?\Z/is", $query, $matches)) {
            list(, $ifExists, $dbName, $sequenceName) = $matches;

            $db = $this->get_database($dbName);
            if($db === false) {
                return false;
            }

            $sequences = $db->getSequences();
            $sequence = false;
            if($sequences->exists()) {
                $sequence = $sequences->getSequence($sequenceName);
            }

            if($sequence === false) {
                if(empty($ifExists)) {
                    return $this->set_error("Sequence {$db->name}.{$sequenceName} does not exist");
                } else {
                    return true;
                }
            }

            $sequences->dropSequence($sequenceName);
            return true;
        } else {
            return $this->set_error("Invalid DROP query");
        }
    }

    private function query_truncate($query)
    {
        if(preg_match("/\ATRUNCATE\s+TABLE\s+(?:`?([A-Z][A-Z0-9\_]*)`?\.)?`?([A-Z][A-Z0-9\_]*)`?(?:\s+(CONTINUE|RESTART)\s+IDENTITY)?\s*[;]?\Z/is", $query, $matches)) {
            list(, $db_name, $table_name, ) = $matches;
            $table = $this->find_table($db_name, $table_name);
            if($table === false) {
                return false;
            } else if($table->isReadLocked()) {
                return $this->error_table_read_lock($table->fullName());
            }

            $table->truncate();
            if(isset($matches[3]) && !strcasecmp($matches[3], 'RESTART')) {
                $identity = $table->getIdentity();
                if($identity !== null) {
                    $identity->restart();
                }
            }
        } else {
            return $this->set_error("Invalid TRUNCATE query");
        }

        return true;
    }

    private function query_backup($query)
    {
        if(preg_match("/\ABACKUP\s+TABLE\s+(.*?)\s+TO\s+'(.*?)'\s*[;]?\Z/is", $query, $matches)) {
            if(substr($matches[2], -1) != "/")
                $matches[2] .= '/';

            $tables = explode(",", $matches[1]);
            foreach($tables as $table) {
                if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_name_matches)) {
                    list(, $db_name, $table_name) = $table_name_matches;

                    $tableObj = $this->find_table($db_name, $table_name);
                    if($tableObj === false) {
                        return false;
                    } else if($tableObj->temporary()) {
                        return $this->set_error("Can not backup a temporary table");
                    }

                    $tableObj->copyTo($matches[2]);
                } else {
                    return $this->set_error("Parse error in table listing");
                }
            }
        } else {
            return $this->set_error("Invalid BACKUP query");
        }
    }

    private function query_restore($query)
    {
        if(preg_match("/\ARESTORE\s+TABLE\s+(.*?)\s+FROM\s+'(.*?)'\s*[;]?\Z/is", $query, $matches)) {
            if(substr($matches[2], -1) != "/")
                $matches[2] .= '/';

            $tables = explode(",", $matches[1]);
            foreach($tables as $table) {
                if(preg_match("/`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?/is", $table, $table_name_matches)) {
                    list(, $db_name, $table_name) = $table_name_matches;

                    $tableObj = $this->find_table($db_name, $table_name);
                    if($tableObj === false) {
                        return false;
                    } else if($tableObj->temporary()) {
                        return $this->set_error("Can not restore a temporary table");
                    }

                    $tableObj->copyFrom($matches[2]);
                } else {
                    return $this->set_error("Parse error in table listing");
                }
            }
        } else {
            return $this->set_error("Invalid RESTORE Query");
        }
    }

    private function query_show($query)
    {
        if(preg_match("/\ASHOW\s+(FULL\s+)?TABLES(?:\s+FROM\s+`?([A-Z][A-Z0-9\_]*)`?)?\s*[;]?\Z/is", $query, $matches)) {
            $full = !empty($matches[1]);
            $db_name = !empty($matches[2]) ? $matches[2] : "";

            $db = $this->get_database($db_name);
            if($db === false) {
                return false;
            }

            $tables = $db->listTables();
            $data = array();

            foreach($tables as $table_name) {
                if($full) {
                    $data[] = array($table_name, 'BASE TABLE');
                } else {
                    $data[] = array($table_name);
                }
            }

            $columns = array("Name");
            if($full) {
                $columns[] = 'Table_type';
            }

            return $this->create_result_set($columns, $data);
        } else if(preg_match("/\ASHOW\s+DATABASES\s*[;]?\s*\Z/is", $query, $matches)) {
            $dbs = array_keys($this->databases);
            foreach($dbs as $db) {
                $data[] = array($db);
            }

            return $this->create_result_set($columns, $data);
        } else if(preg_match('/\ASHOW\s+(FULL\s+)?COLUMNS\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?(?:\s+(?:FROM|IN)\s+`?([^\W\d]\w*)`?)?\s*[;]?\s*\Z/is', $query, $matches)) {
            $db_name = !empty($matches[3]) ? $matches[3] : '';
            return $this->show_columns($db_name, $matches[2], !empty($matches[1]));
         } else {
            return $this->set_error("Invalid SHOW query");
        }
    }

    private function show_columns($db_name, $table_name, $full)
    {
        $table = $this->find_table($db_name, $table_name);
        if($table === false) {
            return false;
        }
        $tableColumns =  $table->getColumns();

        $data = array();

        foreach($tableColumns as $name => $column) {
            $type = $this->typecode_to_name($column['type']);
            $null = ($column['null']) ? 'YES' : '';
            $extra = ($column['auto']) ? 'auto_increment' : '';
            $default = $column['default'];

            if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $default, $matches)) {
                $default = $matches[1];
            }

            if($column['key'] == 'p')
                $key = 'PRI';
            else if($column['key'] == 'u')
                $key = 'UNI';
            else
                $key = '';

            $row = array($name, $type, $null, $column['default'], $key, $extra);
            if ($full) {
                array_splice($row, 2, 0, array(null));
                array_push($row, 'select,insert,update,references', '');
            }

            $data[] = $row;
        }

        $columns = array("Field", "Type", "Null", "Default", "Key", "Extra");
        if($full) {
            array_splice($columns, 2, 0, 'Correlation');
            array_push($columns, "Privileges", "Comment");
        }

        return $this->create_result_set($columns, $data);
    }

    private function query_describe($query)
    {
        if(preg_match("/\ADESC(?:RIBE)?\s+`?(?:([A-Z][A-Z0-9\_]*)`?\.`?)?([A-Z][A-Z0-9\_]*)`?\s*[;]?\Z/is", $query, $matches)) {
            return $this->show_columns($matches[1], $matches[2], false);
        } else {
            return $this->set_error('Invalid DESCRIBE query');
        }
    }

    private function query_use($query)
    {
        if(preg_match("/\AUSE\s+`?([A-Z][A-Z0-9\_]*)`?\s*[;]?\Z/is", $query, $matches)) {
            $this->select_db($matches[1]);
            return true;
        } else {
            return $this->set_error('Invalid USE query');
        }
    }

    private function query_lock($query)
    {
        if(preg_match("/\ALOCK\s+TABLES\s+(.+?)\s*[;]?\Z/is", $query, $matches)) {
            preg_match_all("/(?:([A-Z][A-Z0-9\_]*)`?\.`?)?`?([A-Z][A-Z0-9\_]*)`?\s+((?:READ(?:\s+LOCAL)?)|((?:LOW\s+PRIORITY\s+)?WRITE))/is", $matches[1], $rules);
            $numRules = count($rules[0]);
            for($r = 0; $r < $numRules; $r++) {
                $db_name = $rules[1][$r];
                $table_name = $rules[2][$r];

                $table = $this->find_table($db_name, $table_name);
                if($table !== false) {
                    return false;
                }

                if(!strcasecmp(substr($rules[3][$r], 0, 4), "READ")) {
                    $table->readLock();
                }
                else {  /* WRITE */
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
        if(preg_match("/\AUNLOCK\s+TABLES\s*[;]?\Z/is", $query)) {
            $this->unlock_tables();
            return true;
        } else {
            return $this->set_error('Invalid UNLOCK query');
        }
    }

    public function create_result_set($columns, $data)
    {
        $rs_id = !empty($this->data) ? max(array_keys($this->data)) + 1 : 1;
        $this->columns[$rs_id] = $columns;
        $this->cursors[$rs_id] = array(0, 0);
        $this->data[$rs_id] = $data;
        return $rs_id;
    }

    private function parse_value($columnDef, $value)
    {
        // Blank, NULL, or DEFAULT values
        if(!strcasecmp($value, 'NULL') || strlen($value) === 0 || !strcasecmp($value, 'DEFAULT')) {
            return !$columnDef['null'] ? $columnDef['default'] : null;
        }

        switch($columnDef['type']) {
            case FSQL_TYPE_INTEGER:
                if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
                    return (int) $matches[1];
                }
                else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
                    return (int) $value;
                }
                else {
                    return $this->set_error('Invalid integer value for insert');
                }
            case FSQL_TYPE_FLOAT:
                if(preg_match("/\A'\s*((?:[\+\-]\s*)?\d+(?:\.\d+)?)\s*'\Z/is", $value, $matches)) {
                    return (float) $matches[1];
                }
                else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $value)) {
                    return (float) $value;
                }
                else {
                    return $this->set_error('Invalid float value for insert');
                }
            case FSQL_TYPE_ENUM:
                if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $matches)) {
                    $value = $matches[1];
                }

                if(in_array($value, $columnDef['restraint']) || strlen($value) === 0) {
                    return $value;
                } else if(is_numeric($value)) {
                    $index = (int) $value;
                    if($index >= 1 && $index <= count($columnDef['restraint'])) {
                        return $columnDef['restraint'][$index - 1];
                    } else if($index === 0) {
                        return "";
                    } else {
                        return $this->set_error('Numeric ENUM value out of bounds');
                    }
                } else {
                    return $columnDef['default'];
                }
            case FSQL_TYPE_DATE:
                list($year, $month, $day) = array('0000', '00', '00');
                if(preg_match("/\A'((?:[1-9]\d)?\d{2})-(0[1-9]|1[0-2])-([0-2]\d|3[0-1])(?: (?:[0-1]\d|2[0-3]):(?:[0-5]\d):(?:[0-5]\d))?'\Z/is", $value, $matches)
                || preg_match("/\A'((?:[1-9]\d)?\d{2})(0[1-9]|1[0-2])([0-2]\d|3[0-1])(?:(?:[0-1]\d|2[0-3])(?:[0-5]\d)(?:[0-5]\d))?'\Z/is", $value, $matches)) {
                    list(, $year, $month, $day) = $matches;
                } else {
                    list($year, $month, $day) = array('0000', '00', '00');
                }
                if(strlen($year) === 2)
                    $year = ($year <= 69) ? 2000 + $year : 1900 + $year;
                return $year.'-'.$month.'-'.$day;
            default:
                if(preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $value, $matches)) {
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
        foreach($paramList as $match)
        {
            $expr = $this->build_expression($match, $join_info, $where_type);
            if($expr === false) // parse error
                return false;
            $params[] = $expr;
        }
        return $params;
    }

    private function parse_extract_function($join_info, $where_type, $paramsStr)
    {
        if(preg_match("/\A\s*(\w+)\s+FROM\s+(.+?)\s*\Z/is", $paramsStr, $matches))
        {
            $field = strtolower($matches[1]);
            $field = "'$field'";
            $expr = $this->build_expression($matches[2], $join_info, $where_type);
            if($expr !== false)
            {
                return array('extract', array($field, $expr));
            }
        }

        return $this->set_error('Error parsing extract() function parameters');
    }

    private function parse_overlay_function($join_info, $where_type, $paramsStr)
    {
        if(preg_match("/\A\s*(.+?)\s+PLACING\s+(.+?)\s+FROM\s+(.+?)(?:\s+FOR\s+(.+?))?\s*\Z/is", $paramsStr, $matches))
        {
            $params = $this->build_parameters($join_info, $where_type, $matches);
            return $params !== false ? array('overlay', $params) : false;
        }
        else
            return $this->environment->set_error('Error parsing overlay() function parameters');
    }

    private function parse_position_function($join_info, $where_type, $params)
    {
        if(preg_match("/\A\s*(.+?)\s+IN\s+(.+?)\s*\Z/is", $params, $matches))
        {
            $substring = $this->build_expression($matches[1], $join_info, $where_type);
            $string = $this->build_expression($matches[2], $join_info, $where_type);
            if($substring !== false && $string !== false)
            {
                return array('position', array($substring, $string));
            }
        }

        return $this->set_error('Error parsing position() function parameters');
    }

    private function parse_substring_function($join_info, $where_type, $paramsStr)
    {
        if(preg_match("/\A\s*(.+?)\s+FROM\s+(.+?)(?:\s+FOR\s+(.+?))?\s*\Z/is", $paramsStr, $matches))
        {
            $params = $this->build_parameters($join_info, $where_type, $matches);
            return $params !== null ? array('substring', $params) : false;
        }
        else
            return $this->set_error('Error parsing substring() function parameters');
    }

    private function parse_trim_function($join_info, $where_type, $paramsStr)
    {
        if(preg_match("/\A\s*(?:(?:(LEADING|TRAILING|BOTH)\s+)?(?:(.+?)\s+)?FROM\s+)?(.+?)\s*\Z/is", $paramsStr, $matches))
        {
            switch(strtoupper($matches[1]))
            {
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
            if($string === null)
                return false;
            $params = array($string);

            if(!empty($matches[2]))
            {
                $characters = $this->build_expression($matches[2], $join_info, $where_type);
                if($characters === null)
                    return false;
                $params[] = $characters;
            }

            return array($function, $params);
        }
        else
            return $this->set_error('Error parsing trim() function parameters');
    }


    private function inner_join($left_data, $right_data, $join_comparator)
    {
        if(empty($left_data) || empty($right_data))
            return array();

        $new_join_data = array();

        foreach($left_data as $left_entry)
        {
            foreach($right_data as $right_entry) {
                if($join_comparator($left_entry, $right_entry)) {
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

        foreach($left_data as $left_row => $left_entry)
        {
            $match_found = false;
            foreach($right_data as $right_row => $right_entry) {
                if($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $joinMatches[$left_row] = $right_row;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }

            if(!$match_found) {
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

        foreach($right_data as $right_entry)
        {
            $match_found = false;
            foreach($left_data as $left_entry) {
                if($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                }
            }

            if(!$match_found)
                $new_join_data[] = array_merge($left_padding, $right_entry);
        }

        return $new_join_data;
    }

    private function full_join($left_data, $right_data, $join_comparator, $left_pad_length, $right_pad_length)
    {
        $new_join_data = array();
        $matched_rids = array();
        $left_padding = array_fill(0, $left_pad_length, null);
        $right_padding = array_fill(0, $right_pad_length, null);

        foreach($left_data as $left_entry)
        {
            $match_found = false;
            foreach($right_data as $rid => $right_entry) {
                if($join_comparator($left_entry, $right_entry)) {
                    $match_found = true;
                    $new_join_data[] = array_merge($left_entry, $right_entry);
                    if(!in_array($rid, $matched_rids))
                        $matched_rids[] = $rid;
                }
            }

            if(!$match_found)
                $new_join_data[] = array_merge($left_entry, $right_padding);
        }

        $unmatched_rids = array_diff(array_keys($right_data), $matched_rids);
        foreach($unmatched_rids as $rid) {
            $new_join_data[] = array_merge($left_padding, $right_data[$rid]);
        }

        return $new_join_data;
    }

    function fetch_all($id, $type = 1)
	{
        if($id && isset($this->cursors[$id]) && isset($this->data[$id])) {
            $data = $this->data[$id];
            if($type === FSQL_NUM) {
                return $data;
            }

            $result_array = array();
            $columns = $this->columns[$id];
            if($type === FSQL_ASSOC) {
                foreach($data as $entry)
                    $result_array[] = array_combine($columns, $entry);
            } else {
                foreach($data as $entry)
                    $result_array[] = array_merge($entry, array_combine($columns, $entry));
            }
            return $result_array;
        } else {
            return $this->set_error('Bad results id passed in');
        }
	}

    public function fetch_array($id, $type = 1)
    {
        if(!$id || !isset($this->cursors[$id]) || !isset($this->data[$id][$this->cursors[$id][0]]))
            return false;

        $entry = $this->data[$id][$this->cursors[$id][0]];
        if(!$entry)
            return false;

        $columnNames = $this->columns[$id];

        $this->cursors[$id][0]++;

        if($type === FSQL_ASSOC) {  return array_combine($columnNames, $entry); }
        else if($type === FSQL_NUM) { return $entry; }
        else{ return array_merge($entry, array_combine($columnNames, $entry)); }
    }

    public function fetch_assoc($results) { return $this->fetch_array($results, FSQL_ASSOC); }
    public function fetch_row    ($results) { return $this->fetch_array($results, FSQL_NUM); }
    public function fetch_both    ($results) { return $this->fetch_array($results, FSQL_BOTH); }

    public function fetch_single($results, $column = 0) {
        $type = is_numeric($column) ? FSQL_NUM : FSQL_ASSOC;
        $row = $this->fetch_array($results, $type);
        return $row !== false ? $row[$column] : false;
    }

    public function fetch_object($results)
    {
        $row = $this->fetch_array($results, FSQL_ASSOC);

        if($row === false)
            return false;

        $obj = new stdClass();

        foreach($row as $key => $value)
            $obj->{$key} = $value;

        return $obj;
    }

    public function data_seek($id, $i)
    {
        if(!$id || !isset($this->cursors[$id][0])) {
            return $this->set_error("Bad results id passed in");
        } else {
            $this->cursors[$id][0] = $i;
            return true;
        }
    }

    public function num_fields($id)
    {
        if(!$id || !isset($this->columns[$id])) {
            return $this->set_error("Bad results id passed in");
        } else {
            return count($this->columns[$id]);
        }
    }

    public function fetch_field($id, $i = null)
    {
        if(!$id || !isset($this->columns[$id]) || !isset($this->cursors[$id][1])) {
            return $this->set_error("Bad results id passed in");
        } else {
            if($i === null)
                $i = 0;

            if(!isset($this->columns[$id][$i]))
                return false;

            $field = new stdClass();
            $field->name = $this->columns[$id][$i];
            return $field;
        }
    }

    public function free_result($id)
    {
        unset($this->columns[$id], $this->data[$id], $this->cursors[$id]);
    }

    private function typecode_to_name($type)
    {
        switch($type)
        {
            case FSQL_TYPE_DATE:                return 'DATE';
            case FSQL_TYPE_DATETIME:            return 'DATETIME';
            case FSQL_TYPE_ENUM:                return 'ENUM';
            case FSQL_TYPE_FLOAT:                return 'DOUBLE';
            case FSQL_TYPE_INTEGER:                return 'INTEGER';
            case FSQL_TYPE_STRING:                return 'TEXT';
            case FSQL_TYPE_TIME:                return 'TIME';
            default:                            return false;
        }
    }
}
