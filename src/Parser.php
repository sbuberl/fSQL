<?php

namespace FSQL;

use FSQL\Queries\CreateTableLike;
use FSQL\Queries\Insert;

class Parser
{
    protected $environment;

    public function __construct(Environment $environment)
    {
        $this->environment = $environment;
    }

    public function parse($query) {
        $query = trim($query);
        $function = strstr($query, ' ', true);
        if($function === false) {
            $function = strstr($query, ';', true);
            if($function === false) {
                $function = $query;
            }
        }
        switch (strtoupper($function)) {
            case 'ALTER':       return $this->parseAlter($query);
            case 'BEGIN':       return $this->parseBegin($query);
            case 'COMMIT':      return $this->parseCommit($query);
            case 'CREATE':      return $this->parseCreate($query);
      //      case 'DELETE':      return $this->parseDelete($query);
      //      case 'DESC':
      //      case 'DESCRIBE':    return $this->parseDescribe($query);
      //      case 'DROP':        return $this->parseDrop($query);
      //      case 'INSERT':      return $this->parseInsert($query);
      //      case 'LOCK':        return $this->parseLock($query);
      //      case 'MERGE':       return $this->parseMerge($query);
     //       case 'RENAME':      return $this->parseRename($query);
      //      case 'REPLACE':     return $this->parseReplace($query);
            case 'ROLLBACK':    return $this->parseRollback($query);
     //       case 'SELECT':      return $this->parseSelect($query);
    //        case 'SHOW':        return $this->parseShow($query);
            case 'START':       return $this->parseStart($query);
     //       case 'TRUNCATE':    return $this->parseTruncate($query);
            case 'UNLOCK':      return $this->parseUnlock($query);
     //       case 'UPDATE':      return $this->parseUpdate($query);
     //       case 'USE':         return $this->parseUse($query);
            default:            return $this->environment->set_error('Invalid Query');
        }
    }

    private function parseBasicQuery($query, $name, $pattern, $action)
    {
        if (preg_match($pattern, $query)) {
            return $action();
        } else {
            return $this->environment->set_error('Invalid '. $name . ' query');
        }
    }

    private function parseAlter($query)
    {
        if (preg_match("/\AALTER\s+(TABLE|SEQUENCE)\s+(?:(IF\s+EXISTS)\s+)?(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $type, $ifExists, $definition) = $matches;
            $ifExists = !empty($ifExists);
            if (!strcasecmp($type, 'TABLE')) {
                return $this->parseAlterTable($definition, $ifExists);
            } else {
                return $this->parseAlterSequence($definition, $ifExists);
            }
        } else {
            return $this->environment->set_error('Invalid ALTER query');
        }
    }

    private function parseAlterSequence($definition, $ifExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.+?)\s*[;]?\Z/is", $definition, $matches)) {
            list(, $fullSequenceName, $valuesList) = $matches;
            $seqNamePieces = $this->parseRelationName($fullSequenceName);
            return new Queries\AlterSequence($this->environment, $seqNamePieces, $ifExists, $valuesList);
        } else {
            return $this->environment->set_error('Invalid ALTER SEQUENCE query');
        }
    }

    private function parseAlterTable($definition, $ifExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(.*)/is", $definition, $matches, PREG_OFFSET_CAPTURE)) {
            $fullTableName = $matches[1][0];
            $tableNamePieces = $this->parseRelationName($fullTableName);
            $tableObj = $this->environment->find_table($tableNamePieces);
            if ($tableObj === false) {
                return $ifExists;
            } elseif ($tableObj->isReadLocked()) {
                return $this->environment->error_table_read_lock($tableNamePieces);
            }

            $tableName = $tableNamePieces[2];
            $columns = $tableObj->getColumns();
            $typeRegex = $this->getTypeParseRegex();

            $currentPos = $matches[2][1];
            $stop = false;
            $actions = [];
            while (!$stop && preg_match("/\s*((?:ADD|ALTER|DROP|RENAME).+?)(\s*,\s*|\Z)/Ais", $definition, $colmatches, 0, $currentPos)) {
                $stop = empty($colmatches[2]);

                if (preg_match("/ADD\s+(?:COLUMN\s+)?(?:`?([^\W\d]\w*?)`?(?:\s+({$typeRegex})(?:\((.+?)\))?)\s*(UNSIGNED\s+)?(?:GENERATED\s+(BY\s+DEFAULT|ALWAYS)\s+AS\s+IDENTITY(?:\s*\((.*?)\))?)?(.*?)?(?:,|\)|$))/Ais", $definition, $matches, 0, $currentPos)) {
                    $name = $matches[1];
                    $typeName = $matches[2];
                    $options = $matches[7];

                    if (isset($columns[$name])) {
                        return $this->environment->set_error("Column {$name} already exists");
                    }

                    $type = Types::getTypeCode($typeName);

                    if (preg_match("/\bnot\s+null\b/i", $options)) {
                        $null = 0;
                    } else {
                        $null = 1;
                    }

                    $auto = 0;
                    $restraint = null;
                    if (!empty($matches[5])) {
                        $auto = 1;
                        $always = (int) !strcasecmp($matches[5], 'ALWAYS');
                        $parsed = $this->parseSequenceOptions($matches[6]);
                        if ($parsed === false) {
                            return false;
                        }

                        $restraint = $this->loadCreateSequence($parsed);
                        $start = $restraint[0];
                        array_unshift($restraint, $start, $always);

                        $null = 0;
                    } elseif (preg_match('/\bAUTO_?INCREMENT\b/i', $options)) {
                        $auto = 1;
                        $restraint = array(1, 0, 1, 1, 1, PHP_INT_MAX, 0);
                    }

                    if ($auto) {
                        if ($type !== Types::INTEGER && $type !== Types::FLOAT) {
                            return $this->environment->set_error('Identity columns and autoincrement only allowed on numeric columns');
                        } elseif ($hasIdentity) {
                            return $this->environment->set_error('A table can only have one identity column.');
                        }
                        $hasIdentity = true;
                    }

                    if ($type === Types::ENUM) {
                        $enumList = substr($columns[3], 1, -1);
                        $restraint = preg_split("/'\s*,\s*'/", $enumList);
                    }

                    if (preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')/is", $options, $matches)) {
                        if ($auto) {
                            return $this->environment->set_error('Can not specify a default value for an identity column');
                        }

                        $default = $this->parseDefault($matches[1], $type, $null, $restraint);
                    } else {
                        $default = $this->environment->get_type_default_value($type, $null);
                    }

                    if (preg_match('/(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)/is', $options, $keyMatches)) {
                        $keyType = strtolower($keyMatches[1]);
                        $key = $keyType[0];
                    } else {
                        $key = 'n';
                    }

                    $actions[] = new Queries\AlterTableActions\AddColumn($this->environment, $tableNamePieces, $name, $type, $auto, $default, $key, $null, $restraint);
                } elseif (preg_match("/ADD\s+(?:CONSTRAINT\s+`?[^\W\d]\w*`?\s+)?PRIMARY\s+KEY\s*\((.+?)\)/Ais", $definition, $matches, 0, $currentPos)) {
                    $actions[] = new Queries\AlterTableActions\AddPrimaryKey($this->environment, $tableNamePieces, $matches[1]);
                } elseif (preg_match("/ALTER(?:\s+(?:COLUMN))?\s+`?([^\W\d]\w*)`?\s+(.+?)(?:,|;|\Z)/Ais", $definition, $matches, 0, $currentPos)) {
                    list(, $columnName, $the_rest) = $matches;
                    if (!isset($columns[$columnName])) {
                        return $this->environment->set_error("Column named $columnName does not exist in table $tableName");
                    }

                    $columnDef = $columns[$columnName];
                    if (preg_match("/SET\s+DATA\s+TYPE\s+({$typeRegex})(\s+UNSIGNED)?/is", $the_rest, $types)) {
                        $type = Types::getTypeCode($types[1]);
                        $actions[] = new Queries\AlterTableActions\SetDataType($this->environment, $tableNamePieces, $columnName, $type, $this->environment->get_functions());
                    } else if (preg_match("/(?:SET\s+DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')|DROP\s+DEFAULT)/is", $the_rest, $defaults)) {
                        if(!empty($defaults[1])) {
                            $actions[] = new Queries\AlterTableActions\SetDefault($this->environment, $tableNamePieces, $columnName, $defaults[1]);
                        } else {
                            $actions[] = new Queries\AlterTableActions\DropDefault($this->environment, $tableNamePieces, $columnName);
                        }
                    } elseif (preg_match("/\ADROP\s+IDENTITY/is", $the_rest, $defaults)) {
                        $actions[] = new Queries\AlterTableActions\DropIdentity($this->environment, $tableNamePieces, $columnName);
                    } else {
                        $parsed = $this->parseSequenceOptions($the_rest, true);
                        if ($parsed === false) {
                            return false;
                        } elseif (!empty($parsed)) {
                            $actions[] = new Queries\AlterTableActions\AlterIdentity($this->environment, $tableNamePieces, $columnName, $parsed);
                        }
                    }
                } elseif (preg_match("/DROP\s+(?:COLUMN\s+)?`?([^\W\d]\w*)`?\s*(?:,|;|\Z)/Ais", $definition, $matches, 0, $currentPos)) {
                    $actions[] = new Queries\AlterTableActions\DropColumn($this->environment, $tableNamePieces, $matches[1]);
                } elseif (preg_match("/DROP\s+PRIMARY\s+KEY/Ais", $definition, $matches, 0, $currentPos)) {
                    $actions[] = new Queries\AlterTableActions\DropPrimaryKey($this->environment, $tableNamePieces);
                } elseif (preg_match("/RENAME\s+(?:TO\s+)?(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/Ais", $definition, $matches, 0, $currentPos)) {
                    $newTableNamePieces = $this->parseRelationName($matches[1]);
                    if ($newTableNamePieces === false) {
                        return false;
                    }
                    $actions[] = new Queries\RenameTable($this->environment, $tableNamePieces, $newTableNamePieces);
                } else {
                    return $this->environment->set_error('Invalid ALTER TABLE query');
                }

                $currentPos += strlen($colmatches[0]);
            }

            return new Queries\AlterTable($this->environment, $tableNamePieces, $actions);
        } else {
            return $this->environment->set_error('Invalid ALTER TABLE query');
        }
    }

    private function parseBegin($query)
    {
        return $this->parseBasicQuery($query, 'BEGIN', '/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is', function () { return new Queries\Begin($this->environment); });
    }

    private function parseCreate($query)
    {
        if (preg_match("/\ACREATE\s+((?:TEMPORARY\s+)?TABLE|(?:S(?:CHEMA|EQUENCE)))\s+(?:(IF\s+NOT\s+EXISTS)\s+)?(.+?)\s*[;]?\Z/is", $query, $matches)) {
            list(, $type, $ifNotExists, $definition) = $matches;
            $type = strtoupper($type);
            $ifNotExists = !empty($ifNotExists);
            if (substr($type, -5) === 'TABLE') {
                $temp = !strncmp($type, 'TEMPORARY', 9);

                $query = $this->parseCreateTable($definition, $temp, $ifNotExists);
            } elseif ($type === 'SCHEMA') {
                $query = $this->parseCreateSchema($definition, $ifNotExists);
            } else {
                $query = $this->parseCreateSequence($definition, $ifNotExists);
            }

            return $query;
        } else {
            return $this->environment->set_error('Invalid CREATE query');
        }
    }

    private function parseCreateSchema($definition, $ifNotExists)
    {
        if (preg_match("/\A(?:`?([^\W\d]\w*)`?\.)?`?([^\W\d]\w*)`?\Z/is", $definition, $matches)) {
            list(, $dbName, $schemaName) = $matches;

            return new Queries\CreateSchema($this->environment, array($dbName, $schemaName), $ifNotExists);
        } else {
            return $this->environment->set_error('Invalid CREATE SCHEMA query');
        }
    }

    private function parseCreateSequence($definition, $ifNotExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+(?:AS\s+[^\W\d]\w*\s*)?(.+)\Z/is", $definition, $matches)) {
            list(, $fullSequenceName, $valuesList) = $matches;
            $seqNamePieces = $this->parseRelationName($fullSequenceName);
            if ($seqNamePieces === false) {
                return false;
            }

            $parsed = $this->parseSequenceOptions($valuesList);
            if ($parsed === false) {
                return false;
            }

            $initialValues = $this->loadCreateSequence($parsed);

            return new Queries\CreateSequence($this->environment, $seqNamePieces, $ifNotExists, $initialValues);
        } else {
            return $this->environment->set_error('Invalid CREATE SEQUENCE query');
        }
    }

    private function parseCreateTable($definition, $temporary, $ifNotExists)
    {
        if (preg_match("/\A(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s*\((.+)\)|\s+LIKE\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+([\w\s}]+))?)\s*[;]?/is", $definition, $matches, PREG_OFFSET_CAPTURE)) {
            $full_table_name = $matches[1][0];
            $column_list = $matches[2][0];

            $table_name_pieces = $this->parseRelationName($full_table_name);
            if ($table_name_pieces === false) {
                return false;
            }

            $typeRegex = $this->getTypeParseRegex();

            $currentPos = $matches[2][1];

            if (!isset($matches[3])) {
                $newColumns = [];
                $hasIdentity = false;
                $queryLength = strlen($definition);
                while ($currentPos < $queryLength) {
                    if (preg_match("/(?:(?:CONSTRAINT\s+(?:`?[^\W\d]\w*`?\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+`?([^\W\d]\w*)`?)?\s*\(`?(.+?)`?\))(?:\s*,\s*|\)|$)/Ais", $definition, $columns, 0, $currentPos)) {
                        $currentPos += strlen($columns[0]);

                        if (!$columns[3]) {
                            return $this->environment->set_error("Parse Error: Excepted column name in \"{$columns[1]}\"");
                        }

                        $keyType = strtolower($columns[1]);
                        if ($keyType === 'index') {
                            $keyType = 'key';
                        }
                        $keyColumns = explode(',', $columns[3]);
                        foreach ($keyColumns as $keyColumn) {
                            $newColumns[trim($keyColumn)]['key'] = $keyType[0];
                        }
                    } else if (preg_match("/(?:`?([^\W\d]\w*?)`?(?:\s+({$typeRegex})(?:\((.+?)\))?)\s*(UNSIGNED\s+)?(?:GENERATED\s+(BY\s+DEFAULT|ALWAYS)\s+AS\s+IDENTITY(?:\s*\((.*?)\))?)?(.*?)?(?:\s*,\s*|\)|$))/Ais", $definition, $columns, 0, $currentPos)) {
                        $currentPos += strlen($columns[0]);

                        $name = $columns[1];
                        $typeName = $columns[2];
                        $options = $columns[7];

                        if (isset($newColumns[$name])) {
                            return $this->environment->set_error("Column '{$name}' redefined");
                        }

                        $type = Types::getTypeCode($typeName);
                        if( $type === false)
                            return $this->environment->set_error("Column '{$name}' has unknown type '{$typeName}'");

                        if (preg_match("/\bnot\s+null\b/i", $options)) {
                            $null = 0;
                        } else {
                            $null = 1;
                        }

                        $auto = 0;
                        $restraint = null;
                        if (!empty($columns[5])) {
                            $auto = 1;
                            $always = (int) !strcasecmp($columns[5], 'ALWAYS');
                            $parsed = $this->parseSequenceOptions($columns[6]);
                            if ($parsed === false) {
                                return false;
                            }

                            $restraint = $this->loadCreateSequence($parsed);
                            $start = $restraint[0];
                            array_unshift($restraint, $start, $always);

                            $null = 0;
                        } elseif (preg_match('/\bAUTO_?INCREMENT\b/i', $options)) {
                            $auto = 1;
                            $restraint = array(1, 0, 1, 1, 1, PHP_INT_MAX, 0);
                        }

                        if ($auto) {
                            if ($type !== Types::INTEGER && $type !== Types::FLOAT) {
                                return $this->environment->set_error('Identity columns and autoincrement only allowed on numeric columns');
                            } elseif ($hasIdentity) {
                                return $this->environment->set_error('A table can only have one identity column.');
                            }
                            $hasIdentity = true;
                        }

                        if ($type === Types::ENUM) {
                            $enumList = substr($columns[3], 1, -1);
                            $restraint = preg_split("/'\s*,\s*'/", $enumList);
                        }

                        if (preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|'.*?(?<!\\\\)')/is", $options, $matches)) {
                            if ($auto) {
                                return $this->environment->set_error('Can not specify a default value for an identity column');
                            }

                            $default = $this->parseDefault($matches[1], $type, $null, $restraint);
                        } else {
                            $default = $this->environment->get_type_default_value($type, $null);
                        }

                        if (preg_match('/(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)/is', $options, $keyMatches)) {
                            $keyType = strtolower($keyMatches[1]);
                            $key = $keyType{0};
                        } else {
                            $key = 'n';
                        }

                        $newColumns[$name] = ['type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint];
                    }
                    else {
                        return $this->environment->set_error('Parsing error in CREATE TABLE query');
                    }
                }

                return new Queries\CreateTable($this->environment, $table_name_pieces, $ifNotExists, $temporary, $newColumns);
            } else {
                $likeClause = isset($matches[4][0]) ? $matches[4][0] : '';
                $likeTablePieces = $this->parseRelationName($matches[3][0]);
                if ($likeTablePieces === false) {
                    return false;
                }

                $likeOptions = $this->parseTableLikeClause($likeClause);
                if ($likeOptions === false) {
                    return false;
                }

                return new Queries\CreateTableLike($this->environment, $table_name_pieces, $ifNotExists, $temporary, $likeTablePieces, $likeOptions);
            }
        } else {
            return $this->environment->set_error('Invalid CREATE TABLE query');
        }
    }

    private function parseCommit($query)
    {
        return $this->parseBasicQuery($query, 'COMMIT', '/\ACOMMIT(?:\s+WORK)?\s*[;]?\Z/is', function () { return new Queries\Commit($this->environment); });
    }

    private function parseRollback($query)
    {
        return $this->parseBasicQuery($query, 'ROLLBACK', '/\AROLLBACK(?:\s+WORK)?\s*[;]?\Z/is', function () { return new Queries\Rollback($this->environment); });
    }

    private function parseStart($query)
    {
        return $this->parseBasicQuery($query, 'START', '/\ASTART\s+TRANSACTION\s*[;]?\Z/is', function () { return new Queries\Begin($this->environment); });
    }

    private function parseUnlock($query)
    {
        return $this->parseBasicQuery($query, 'UNLOCK', '/\AUNLOCK\s+TABLES\s*[;]?\Z/is', function () { return new Queries\Unlock($this->environment); });
    }

    public function parseRelationName($name)
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
            return $this->environment->set_error('Parse error in table name: '.$name);
        }
    }

    public function parseDefault($default, $type, $null, $restraint)
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
                        return $this->environment->set_error('Numeric ENUM value out of bounds');
                    }
                } elseif ($type == Types::STRING) {
                    $default = "'".$matches[1]."'";
                }
            }
        } elseif (!$null) {
            $default = $this->environment->get_type_default_value($type, 0);
        }

        return $default;
    }

    private function getTypeParseRegex()
    {
        return '(?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET';
    }

    private function parseTableLikeClause($likeClause)
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
                return $this->environment->set_error('Unexpected token in LIKE clause: '.$firstWord);
            }

            $word = $optionsWords[++$i];
            if ($word === 'IDENTITY') {
                $type = CreateTableLike::IDENTITY;
            } elseif ($word === 'DEFAULTS') {
                $type = CreateTableLike::DEFAULTS;
            } else {
                return $this->environment->set_error('Unknown option after '.$firstWord.': '.$word);
            }

            $results[$type] = $including;
        }

        return $results;
    }

    private function loadCreateSequence($parsed)
    {
        $increment = isset($parsed['INCREMENT']) ? (int) $parsed['INCREMENT'] : 1;
        if ($increment === 0) {
            return $this->environment->set_error('Increment of zero in identity column defintion is not allowed');
        }

        $climbing = $increment > 0;
        $min = isset($parsed['MINVALUE']) ? (int) $parsed['MINVALUE'] : ($climbing ? 1 : PHP_INT_MIN);
        $max = isset($parsed['MAXVALUE']) ? (int) $parsed['MAXVALUE'] : ($climbing ? PHP_INT_MAX : -1);
        $cycle = isset($parsed['CYCLE']) ? (int) $parsed['CYCLE'] : 0;

        if (isset($parsed['START'])) {
            $start = (int) $parsed['START'];
            if ($start < $min || $start > $max) {
                return $this->environment->set_error('Identity column start value not inside valid range');
            }
        } elseif ($climbing) {
            $start = $min;
        } else {
            $start = $max;
        }

        return array($start, $increment, $min, $max, $cycle);
    }

    public function parseSequenceOptions($options, $isAlter = false)
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
                            return $this->environment->set_error('Unknown option after SET: '.$word);
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
                                return $this->environment->set_error('Expected DEFAULT after BY');
                            }
                            $parsed['ALWAYS'] = false;
                        } elseif ($word === 'ALWAYS') {
                            $parsed['ALWAYS'] = true;
                        } else {
                            return $this->environment->set_error('Unexpected word after GENERATED: ' + $word);
                        }
                    }
                }

                if (in_array($word, $valueTypes)) {
                    $original = $word;
                    if (isset($secondWords[$original])) {
                        $word = $optionsWords[++$i];
                        $second = $secondWords[$original];
                        if ($word !== $second) {
                            return $this->environment->set_error('Expected '.$second.' after '.$original);
                        }
                    }

                    $word = $optionsWords[++$i];
                    if (preg_match('/[+-]?\s*\d+(?:\.\d+)?/', $word, $number)) {
                        if (!isset($parsed[$original])) {
                            $parsed[$original] = $number[0];
                        } else {
                            return $this->environment->set_error($original.' already set for this identity/sequence.');
                        }
                    } else {
                        return $this->environment->set_error('Could not parse number after '.$original);
                    }
                } elseif ($word === 'NO') {
                    $word = $optionsWords[++$i];
                    if (in_array($word, array('CYCLE', 'MAXVALUE', 'MINVALUE'))) {
                        if (!isset($parsed[$word])) {
                            $parsed[$word] = null;
                        } else {
                            return $this->environment->set_error($word.' already set for this identity column.');
                        }
                    } else {
                        return $this->environment->set_error('Unknown option after NO: '.$word);
                    }
                } elseif ($word === 'CYCLE') {
                    $parsed[$word] = 1;
                }
            }
        }

        return $parsed;
    }
}
