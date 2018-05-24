<?php

namespace FSQL\Statements;


use FSQL\Environment;
use FSQL\OrderByClause;
use FSQL\ResultSet;
use FSQL\Utilities;

class Select extends Statement
{
    private $selectedInfo;

    private $joins;

    private $joinedInfo;

    private $where;

    private $groupList;

    private $having;

    private $orderBy;

    private $limit;

    private $distinct;

    private $isGrouping;

    private $singleRow;

    function  __construct(Environment $environment, $selectedInfo, $joins, $joinedInfo, $where, $groupList, $having, $orderBy, $limit, $distinct, $isGrouping, $singleRow)
    {
        parent:: __construct($environment);
        $this->selectedInfo = $selectedInfo;
        $this->joins = $joins;
        $this->joinedInfo = $joinedInfo;
        $this->where = $where;
        $this->groupList = $groupList;
        $this->having = $having;
        $this->orderBy = $orderBy;
        $this->limit = $limit;
        $this->distinct = $distinct;
        $this->isGrouping = $isGrouping;
        $this->singleRow = $singleRow;
    }

    public function execute()
    {
        $data = $this->getData();

        $selected_columns = [];
        $group_code = '';
        $grouped_set = [];
        if ($this->isGrouping) {
            $group = [];
            list($selected_columns, $line, $group_code) = $this->buildGroupValues();
        } else {
            list($selected_columns, $line) = $this->buildNormalValues();
            $group = $data;
        }

        if (!empty($this->joins)) {
            if ($this->where !== null) {
                $line = "if({$this->where}) {\r\n\t\t\t\t\t$line\r\n\t\t\t\t}";
            }

            $code = <<<EOT
            foreach(\$data as \$entry) {
                $line
            }
$group_code
EOT;
        } else { // Tableless SELECT
            $entry = [true];  // hack so it passes count and !empty expressions
            $data = [$entry];
            $code = $line;
        }

        $final_set = [];

        $evalFunction = function() use($code, $data, $group, $grouped_set, &$final_set){ eval($code); };
        $evalFunction = $evalFunction->bindTo($this->environment);
        $evalFunction();

        if (!empty($this->orderBy)) {
            if (!$this->orderBy($final_set, $selected_columns))
                return false;
        }

        if ($this->limit !== null) {
            $stop = $this->limit[1];
            if ($stop !== null) {
                $final_set = array_slice($final_set, $this->limit[0], $stop);
            } else {
                $final_set = array_slice($final_set, $this->limit[0]);
            }
        }

        return new ResultSet($selected_columns, $final_set);
    }

    private function getData()
    {
        $data = [];
        foreach($this->joins as $alias => $join) {
            $baseTable = $this->environment->find_table($join['fullName']);
            if($baseTable === false)
                return false;
            $baseTableColumns = $baseTable->getColumns();
            $joinColumnsSize = count($baseTableColumns);
            $joinData = $baseTable->getEntries();

            foreach($join['joined'] as $join_op) {
                $joiningTable = $this->environment->find_table($join_op['fullName']);
                if($joiningTable === false)
                    return false;
                $joiningTableColumns = $joiningTable->getColumns();
                $joiningTableColumnsSize = count($joiningTableColumns);

                switch($join_op['type'])
                {
                    default:
                        $joinData = Utilities::innerJoin($joinData, $joiningTable->getEntries(), $join_op['comparator']);
                        break;
                    case 'left':
                        $joinMatches = [];
                        $joinData = Utilities::leftJoin($joinData, $joiningTable->getEntries(), $join_op['comparator'], $joiningTableColumnsSize, $joinMatches);
                        break;
                    case 'right':
                        $joinData = Utilities::rightJoin($joinData, $joiningTable->getEntries(), $join_op['comparator'], $joinColumnsSize);
                        break;
                    case 'full':
                        $joinData = Utilities::fullJoin($joinData, $joiningTable->getEntries(), $join_op['comparator'], $joinColumnsSize, $joiningTableColumnsSize);
                        break;
                }
                $joinColumnsSize += $joiningTableColumnsSize;
            }

            // implicit CROSS JOINs
            if(!empty($joinData)) {
                if(!empty($data)) {
                    $newData = [];
                    foreach($data as $left_entry)
                    {
                        foreach($joinData as $right_entry) {
                            $newData[] = array_merge($left_entry, $right_entry);
                        }
                    }
                    $data = $newData;
                }
                else
                    $data = $joinData;
            }
        }
        return $data;
    }

    private function buildNormalValues()
    {
        $select_line = '';
        $selected_columns = [];
        foreach ($this->selectedInfo as $info) {
            list($select_type, $select_value, $select_alias) = $info;
            switch ($select_type) {
            // function call
            case 'function':
                $expr = $this->environment->build_expression($select_value, $this->joinedInfo, false);
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
                    $star_tables = !empty($table_name) ? [$table_name] : array_keys($this->joinedInfo['tables']);
                    foreach ($star_tables as $tname) {
                        $start_index = $this->joinedInfo['offsets'][$tname];
                        $table_columns = $this->joinedInfo['tables'][$tname];
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
                    $index = $this->environment->find_column($column, $table_name, $this->joinedInfo, 'SELECT clause');
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
                return $this->environment->set_error("Parse Error: Unknown value in SELECT clause: $column");
            }
        }

        $line = '$final_set[] = array('.substr($select_line, 0, -2).');';
        return [$selected_columns, $line];
    }

    private function buildGroupValues()
    {
        $group_key_list = '';
        foreach ($this->groupList as $groupColumn) {
            $group_key_list .= '$entry['.$groupColumn.'], ';
        }

        $group_key = substr($group_key_list, 0, -2);
        if (count($this->groupList) > 1) {
            $group_key = 'serialize(['.$group_key.'])';
        }

        $select_line = '';
        $selected_columns = [];
        foreach ($this->selectedInfo as $info) {
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
                        $index = $this->environment->find_column($column, $table_name, $this->joinedInfo, 'SELECT clause');
                        if ($index === false) {
                            return false;
                        }
                    }

                    if (!in_array($index, $this->groupList)) {
                        return $this->environment->set_error("Selected column '{$this->joinedInfo['columns'][$index]}' is not a grouped column");
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
                    $expr = $this->environment->build_expression($select_value, $this->joinedInfo, Environment::$WHERE_NORMAL);
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

        if (!$this->singleRow) {
            $line = '$grouped_set['.$group_key.'][] = $entry;';
        } else {
            $line = '$group[] = $entry;';
        }

        $final_line = '$final_set[] = array('.substr($select_line, 0, -2).');';

        if ($this->having !== null) {
            $final_line = "if({$this->having}) {\r\n\t\t\t\t\t\t$final_line\r\n\t\t\t\t\t}";
        }

        if (!$this->singleRow) {
            $group_code = <<<EOT
            foreach(\$grouped_set as \$group) {
                $final_line
            }
EOT;
        } else {
            $group_code = $final_line;
        }

        return [$selected_columns, $line, $group_code];
    }

    private function orderBy(array &$final_set, array $selected_columns)
    {
        foreach ($this->orderBy as &$sort) {
            $key = $sort['key'];

            if (is_int($key)) {
                if (!isset($selected_columns[$key])) {
                    return $this->environment->set_error('ORDER BY: Invalid column number: '.($key + 1));
                }
            } else {
                list($table_name, $column_name) = $key;
                $index = array_search($column_name, $selected_columns);
                if ($index === null) {
                    return $this->environment->set_error('ORDER BY: column/alias not in the SELECT list: '.$column_name);
                }
                $sort['key'] = $index;
            }
        }

        $orderBy = new OrderByClause($this->orderBy);
        $orderBy->sort($final_set);
        return true;
    }
}
