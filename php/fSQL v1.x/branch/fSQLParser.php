<?php

define('FSQL_JOIN_INNER',0,TRUE);
define('FSQL_JOIN_LEFT',1,TRUE);
define('FSQL_JOIN_RIGHT',2,TRUE);
define('FSQL_JOIN_FULL',3,TRUE);

class fSQLParser
{
	var $environment;
	
	function loadQueryClass($command)
	{
		return $this->loadExtensionQueryClass($command, null);
	}
	
	function loadExtensionQueryClass($command, $extension)
	{
		$command = ucfirst($command);
		$className = 'fSQL'.$command.'Query';
		if(!class_exists($className))
		{
			if($extension === null)
				require FSQL_INCLUDE_PATH."/queries/$className.php";
			else
				require FSQL_EXTENSIONS_PATH."/$extension/queries/$className.php";
		}
	}
	
	function parseCommandName($query)
	{
		preg_match("/\A[A-Z]+/i", $query, $function);
		return strtolower($function[0]);
	}
	
	function parse(&$environment, $query)
	{
		$this->environment =& $environment;
		$query = trim($query);
		$command = fSQLParser::parseCommandName($query);
		return $this->parseQuery($command, $query);
	}
	
	function parseQuery($command, $query)
	{
		switch($command)
		{
			case 'begin':
				return $this->parseBegin($query);
			case 'create':
				return $this->parseCreate($query);
			case 'commit':
				return $this->parseCommit($query);
			case 'delete':
				return $this->parseDelete($query);
			case 'drop':
				return $this->parseDrop($query);
			case 'insert':
			case 'replace':
				return $this->parseInsert($query);
			case 'rollback':
				return $this->parseRollback($query);
			case 'select':
				return $this->parseSelect($query);
			case 'start':
				return $this->parseStart($query);
			case 'update':
				return $this->parseUpdate($query);
			default:
				return $this->environment->_set_error('Invalid Query: '.$command);
		}
	}
	
	function buildWhere($statement, $join_info, $where_type = FSQL_WHERE_NORMAL)
	{
		if($statement) {
			preg_match_all("/(\A\s*|\s+(?:AND|OR)\s+)(NOT\s+)?(\S+?)(\s*(?:!=|<>|>=|<=>?|>|<|=)\s*|\s+(?:IS(?:\s+NOT)?|(?:NOT\s+)?IN|(?:NOT\s+)?R?LIKE|(?:NOT\s+)?REGEXP)\s+)(\((.*?)\)|'.*?'|\S+)/is", $statement, $WHERE);
			
			$where_count = count($WHERE[0]);
			if($where_count === 0)
				return null;
			
			$condition = "";
						
			for($i = 0; $i < $where_count; ++$i) {
				$local_condition = "";
				$logicalOp = trim($WHERE[1][$i]);
				$not = !empty($WHERE[2][$i]);
				$leftStr = $WHERE[3][$i];
				$operator = preg_replace("/\s+/", " ", trim(strtoupper($WHERE[4][$i])));
				$rightStr = $WHERE[5][$i];
				
				$left = $this->buildExpression($leftStr, $join_info, $where_type);
				if($left === null)
					return null;
				
				$leftExpr = $left['expression'];
				
				if($operator !== "IN" && $operator !== 'NOT IN')
				{
					$right = $this->buildExpression($rightStr, $join_info, $where_type);
					if($right === null)
						return null;

					$rightExpr = $right['expression'];
					$left_nullable = $left['type']['null'];
					$right_nullable = $right['type']['null'];

					if($left_nullable && $right_nullable)
						$nullcheck = "nullcheck";
					else if($left_nullable)
						$nullcheck = "nullcheck_left";
					else if($right_nullable)
						$nullcheck = "nullcheck_right";
					else
						$nullcheck = null;
					
					switch($operator) {
						case '=':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_eq($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '!=':
						case '<>':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_ne($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr != $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '>':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_gt($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr > $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '>=':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_ge($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr >= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_lt($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr < $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<=':
							if($nullcheck)
								$local_condition = "fSQLTypes::_{$nullcheck}_le($leftExpr, $rightExpr)";
							else
								$local_condition = "(($leftExpr <= $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case '<=>':
							$local_condition .= "(($leftExpr == $rightExpr) ? FSQL_TRUE : FSQL_FALSE)";
							break;
						case 'IS NOT':
							$not = !$not;
						case 'IS':
							if($rightExpr === 'NULL')
								$local_condition = "($leftExpr === NULL ? FSQL_TRUE : FSQL_FALSE)";
							else if($rightExpr === 'TRUE')
								$local_condition = "($leftExpr == TRUE ? FSQL_TRUE : FSQL_FALSE)";
							else if($rightExpr === 'FALSE')
								$local_condition = "(in_array($leftExpr, array(0, 0.0, ''), true) ? FSQL_TRUE : FSQL_FALSE)";
							else
								return null;
							break;
						case 'NOT LIKE':
							$not = !$not;
						case 'LIKE':
							$local_condition = "fSQLEnvironment::_fsql_like($leftExpr, $rightExpr)";
							break;
						case 'NOT RLIKE':
						case 'NOT REGEXP':
							$not = !$not;
						case 'RLIKE':
						case 'REGEXP':
							$local_condition = "fSQLEnvironment::_fsql_regexp($leftExpr, $rightExpr)";
							break;
						default:
							$local_condition = "$leftExpr $operator $rightExpr";
							break;
					}
				}
				else
				{
					if(!empty($WHERE[6][$i])) {
						$array_values = explode(',', $WHERE[6][$i]);
						$valuesExpressions = array();
						foreach($array_values as $value)
						{
							$valueExpr = $this->buildExpression(trim($value), $join_info, $where_type);
							$valuesExpressions[] = $valueExpr['expression'];
						}
						$valuesString = implode(',', $valuesExpressions);
						$local_condition = "fSQLEnvironment::_fsql_in($leftExpr, array($valuesString))";
						
						if($operator === 'NOT IN')
							$not = !$not;
					}
					else
						return null;
				}
				
				if(!strcasecmp($logicalOp, 'AND'))
					$condition .= ' & ';
				else if(!strcasecmp($logicalOp, 'OR'))
					$condition .= ' | ';
				
				if($not)
					$condition .= 'fSQLEnvironment::_fsql_not('.$local_condition.')';
				else
					$condition .= $local_condition;
			}
			return "($condition) === ".FSQL_TRUE;
		}
		return null;
	}
 
	function buildExpression($exprStr, $join_info, $where_type = FSQL_WHERE_NORMAL)
	{
		$expr = null;
		$columnData = null;
		
		// function call
		if(preg_match("/\A([^\W\d]\w*)\s*\((.*?)\)/is", $exprStr, $matches)) {
			$function = strtolower($matches[1]);
			$params = $matches[2];
			$final_param_list = '';
			$function_info = null;
			$paramExprs = array();
			
			if(isset($this->registered_functions[$function])) {
				$builtin = false;
				$type = FSQL_TYPE_STRING; // ?
				$function_type = FSQL_FUNC_NORMAL;
			} else {
				if(!class_exists('fSQLFunctions'))
					require_once FSQL_INCLUDE_PATH.'/fSQLFunctions.php';
				
				if(($function_info = fSQLFunctions::getFunctionInfo($function)) !== null) {
					$builtin = true;
					list($function_type, $type, $nullable) = $function_info;
					$columnData = array('type' => $type, 'default' => null, 'null' => $nullable, 'key' => 'n', 'auto' => false, 'restraint' => array());
					switch($function_type)
					{
						case FSQL_FUNC_AGGREGATE:
							$paramExprs[] = '$group';
							break;
						case FSQL_FUNC_ENV:
							$paramExprs[] = '$this->environment';
							break;
					}
				}
				else {
					$this->_set_error('Call to unknown SQL function');
					return null;
				}
			}

			if(!empty($params)) {
				$parameter = explode(',', $params);
				foreach($parameter as $param) {
					$param = trim($param);
					if($function_type === FSQL_FUNC_AGGREGATE && $param === '*' )
					{
						$paramExprs[] = '"*"';
					}
					else
					{	
						$paramExpr = $this->buildExpression($param, $join_info, $where_type | 1);
						$pexp = $paramExpr['expression'];
						if($function_type === FSQL_FUNC_AGGREGATE && preg_match('/\\$entry\[(\d+)\]/', $pexp, $pexp_matches))
							$paramExprs[] = $pexp_matches[1];
						else
							$paramExprs[] = $pexp;
					}
				}
			}
			
			$final_param_list = implode(",", $paramExprs);

			if($builtin)
				$expr = "fSQLFunctions::$function($final_param_list)";
			else
				$expr = "$function($final_param_list)";
		}
		// column/alias/keyword
		else if(preg_match("/\A(?:([^\W\d]\w*|\{\{left\}\})\.)?([^\W\d]\w*)\Z/is", $exprStr, $matches)) {
			list( , $table_name, $column) =  $matches;
			// table.column
			if($table_name) {
				if(isset($join_info['tables'][$table_name])) {
					$table_columns = $join_info['tables'][$table_name];
					if(isset($table_columns[ $column ])) {
						$columnData = $table_columns[ $column ];
						if( isset($join_info['offsets'][$table_name]) ) {
							$colIndex = array_search($column,  array_keys($table_columns)) + $join_info['offsets'][$table_name];
							$expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
						} else {
							$colIndex = array_search($column, array_keys($table_columns));
							$expr = "\$right_entry[$colIndex]";
						}
					}
				}
				else if($where_type & FSQL_WHERE_ON && $table_name === '{{left}}')
				{
					$colIndex = array_search($column, $join_info['columns']);
					$expr = "\$left_entry[$colIndex]";
				}
			}
			// null/unknown
			else if(!strcasecmp($exprStr, 'NULL')  || !strcasecmp($exprStr, 'UNKNOWN')) {
				$expr = 'NULL';
			}
			// true/false
			else if(!strcasecmp($exprStr, 'TRUE') || !strcasecmp($exprStr, 'FALSE')) {
				$expr = strtoupper($exprStr);
				$nullable = false;
			}
			else if($where_type === FSQL_WHERE_HAVING) { // column/alias in grouping clause
				$colIndex = array_search($column, $join_info['columns']);
				if(in_array($colIndex, $join_info['group_columns'])) {
					$owner_table_name = null;
					foreach($join_info['tables'] as $join_table_name => $join_table)
					{
						if($colIndex >= $join_info['offsets'][$join_table_name])
							$owner_table_name = $join_table_name;
						else
							break;
					}
					$columnData = $join_info['tables'][$owner_table_name][$column];
					$expr = "\$group[0][$colIndex]";
				}
			}
			else {  // column/alias
				$colIndex = array_search($column, $join_info['columns']);
				if($colIndex === false)
					return null;

				$owner_table_name = null;
				foreach($join_info['tables'] as $join_table_name => $join_table)
				{
					if($colIndex >= $join_info['offsets'][$join_table_name])
						$owner_table_name = $join_table_name;
					else
						break;
				}
				$columnData = $join_info['tables'][$owner_table_name][$column];
				$expr = ($where_type & FSQL_WHERE_ON) ? "\$left_entry[$colIndex]" : "\$entry[$colIndex]";
			}
		}
		// number
		else if(preg_match("/\A(?:[\+\-]\s*)?\d+(?:\.\d+)?\Z/is", $exprStr)) {
			$expr = $exprStr;
			$type = strpos($exprStr, '.') === false ? FSQL_TYPE_INTEGER : FSQL_TYPE_FLOAT;
			$columnData = array('type' => $type, 'default' => null, 'null' => false, 'key' => 'n', 'auto' => false, 'restraint' => array());
		}
		// string
		else if(preg_match("/\A'.*?(?<!\\\\)'\Z/is", $exprStr)) {
			$expr = $exprStr;
			$columnData = array('type' => FSQL_TYPE_STRING, 'default' => null, 'null' => false, 'key' => 'n', 'auto' => false, 'restraint' => array());
		}
		else if(($where_type & FSQL_WHERE_ON) && preg_match("/\A{{left}}\.([^\W\d]\w*)/is", $exprStr, $matches)) {
			if(($colIndex = array_search($matches[1], $join_info['columns']))) {
				$expr = "\$left_entry[$colIndex]";
			}
		}
		else
			return null;
		
		return array('type' => $columnData, 'expression' => $expr);
	}
	
	function parseBegin($query)
	{
		if(preg_match('/\ABEGIN(?:\s+WORK)?\s*[;]?\Z/is', $query, $matches)) {
			$this->loadQueryClass('start');		
			return new fSQLStartQuery($this->environment);
		} else {
			return $this->environment->_set_error('Invalid Query');
		}
	}
	
	function parseCreate($query)
	{
		if(preg_match('/\ACREATE(?:\s+TEMPORARY)?\s+TABLE\s+/is', $query))
			return $this->parseCreateTable($query);
		else if(preg_match('/\ACREATE(?:\s+OR\s+REPLACE)?\s+VIEW\s+/is', $query))
			return $this->parseCreateView($query);
		else
			return $this->environment->_set_error('Invalid CREATE query');
	}
	
	function parseCreateTable($query)
	{
		if(preg_match('/\ACREATE(?:\s+(TEMPORARY))?\s+TABLE\s+(?:(IF\s+NOT\s+EXISTS)\s+)?(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s*\((.+)\)|\s+LIKE\s+((?:[^\W\d]\w*\.){0,2}[^\W\d]\w*))/is', $query, $matches)) {
			
			list(, $temporary, $ifnotexists, $full_table_name, $column_list) = $matches;

			$table_name_pieces = $this->environment->_parse_table_name($full_table_name);
			if($table_name_pieces === false)
				return false;
			
			$table_name = $table_name_pieces[2];
			$ifnotexists = !empty($ifnotexists);
			$temporary = !empty($temporary);			

			if(!isset($matches[5])) {
				//preg_match_all("/(?:(KEY|PRIMARY KEY|UNIQUE) (?:([^\W\d]\w*)\s*)?\((.+?)\))|(?:`?([^\W\d]\w*?)`?(?:\s+((?:TINY|MEDIUM|BIG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)(\s+UNSIGNED)?(.*?)?(?:,|\)|$))/is", trim($column_list), $Columns);
				preg_match_all('/(?:(?:CONSTRAINT\s+(?:[^\W\d]\w*\s+)?)?(KEY|INDEX|PRIMARY\s+KEY|UNIQUE)(?:\s+([^\W\d]\w*))?\s*\((.+?)\))|(?:`?([^\W\d]\w*?)`?(?:\s+((?:TINY|MEDIUM|LONG)?(?:TEXT|BLOB)|(?:VAR)?(?:CHAR|BINARY)|INTEGER|(?:TINY|SMALL|MEDIUM|BIG)?INT|FLOAT|REAL|DOUBLE(?: PRECISION)?|BIT|BOOLEAN|DEC(?:IMAL)?|NUMERIC|DATE(?:TIME)?|TIME(?:STAMP)?|YEAR|ENUM|SET)(?:\((.+?)\))?)(\s+UNSIGNED\s+)?(.*?)?(?:,|\)|$))/is', trim($column_list), $Columns);

				if(!$Columns) {
					return $this->environment->_set_error('Parsing error in CREATE TABLE query');
				}
				
				$new_columns = array();

				$numMatches = count($Columns[0]);
				for($c = 0; $c < $numMatches; $c++) {
					//$column = str_replace("\"", "'", $column);
					if($Columns[1][$c])
					{
						if(!$Columns[3][$c]) {
							return $this->environment->_set_error("Parse Error: Excepted column name in \"{$Columns[1][$c]}\"");
						}
						
						$keytype = strtolower($Columns[1][$c]);
						if($keytype === 'index')
							$keytype = 'key';
						$keycolumns = explode(',', $Columns[3][$c]);
						foreach($keycolumns as $keycolumn)
						{
							$keycolumn = trim($keycolumn);
							if($new_columns[$keycolumn]['key'] !== 'p')
								$new_columns[$keycolumn]['key'] = $keytype{0}; 
						}
					}
					else
					{
						$name = $Columns[4][$c];
						$type = $Columns[5][$c];
						$options =  $Columns[8][$c];
						
						if(isset($new_columns[$name])) {
							return $this->environment->_set_error("Column '{$name}' redefined");
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
								case 'TIMESTAMP':
									$type = FSQL_TYPE_TIMESTAMP;
									break;
								case 'YEAR':
									$type = FSQL_TYPE_YEAR;
									break;
								default:
									break;
							}
						}
						
						$null = (bool) !preg_match("/\s+not\s+null\b/i", $options);
						
						$auto = (bool) preg_match("/\s+AUTO_INCREMENT\b/i", $options);
						
						if($type === FSQL_TYPE_ENUM) {
							preg_match_all("/'.*?(?<!\\\\)'/", $Columns[6][$c], $values);
							$restraint = $values[0];
						} else {
							$restraint = NULL;
						}
				
						if(preg_match("/DEFAULT\s+((?:[\+\-]\s*)?\d+(?:\.\d+)?|NULL|(\"|').*?(?<!\\\\)(?:\\2))/is", $options, $matches)) {
							$default = $matches[1];
							if(!$null && strcasecmp($default, 'NULL')) {
								if(preg_match("/\A(\"|')(.*)(?:\\1)\Z/is", $default, $matches)) {
									if($type === FSQL_TYPE_INTEGER)
										$default = (int) $matches[2];
									else if($type === FSQL_TYPE_FLOAT)
										$default = (float) $matches[2];
									else if($type === FSQL_TYPE_ENUM) {
										if(in_array($default, $restraint))
											$default = array_search($default, $restraint) + 1;
										else
											$default = 0;
									}
									else
										$default = $matches[2];
								} else {
									if($type === FSQL_TYPE_INTEGER)
										$default = (int) $default;
									else if($type === FSQL_TYPE_FLOAT)
										$default = (float) $default;
									else if($type === FSQL_TYPE_ENUM) {
										$default = (int) $default;
										if($default < 0 || $default > count($restraint)) {
											return $this->_set_error('Numeric ENUM value out of bounds');
										}
									}
								}
							}
							else if(!strcasecmp($default, 'NULL')) {
								$default = null;
							}
						}
						else if($null)
							$default = null;
						else if($type === FSQL_TYPE_STRING)
							// The default for string types is the empty string 
							$default = '';
						else
							// The default for dates, times, and number types is 0
							$default = 0;
				
						if(preg_match('/(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)/is', $options, $keymatches)) {
							$keytype = strtolower($keymatches[1]);
							$key = $keytype{0}; 
						}
						else {
							$key = 'n';
						}
						
						$new_columns[$name] = array('type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint);
					}
				}
			} else {
				$src_table_name_pieces = $this->environment->_parse_table_name($matches[5]);
				$src_table =& $this->environment->_find_table($src_table_name_pieces);
				if($src_table === false)
					return false;
				
				$new_columns = $src_table->getColumns();
			}
			
			$this->loadQueryClass('createTable');
			return new fSQLCreateTableQuery($this->environment, $table_name_pieces, $new_columns, $ifnotexists, $temporary);
		} else {
			return $this->environment->_set_error('Invalid CREATE TABLE query');
		}
	}
	
	function parseCreateView($query)
	{
		if(preg_match('/\ACREATE(\s+OR\s+REPLACE)?\s+VIEW\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+AS\s+(.*)\Z/is', $query, $matches))
		{
			$replace = !empty($matches[1]);
			$view_name_pieces = $this->environment->_parse_table_name($matches[2]);
			$view_query = $matches[3];
			$this->loadQueryClass('createView');
			return new fSQLCreateViewQuery($this->environment, $view_name_pieces, null, $view_query, $replace);
		}
		else
			return $this->environment->_set_error('Invalid CREATE VIEW query');
	}
	
	function parseCommit($query)
	{
		if(preg_match('/\ACOMMIT\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->loadQueryClass('commit');
			return new fSQLCommitQuery($this->environment);
		} else {
			return $this->environment->_set_error('Invalid Query');
		}
	}
	
	function parseDelete($query)
	{
		if(preg_match('/\ADELETE\s+FROM\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(WHERE\s+.+))?\s*[;]?\Z/is', $query, $matches))
		{
			$table_name_pieces = $this->environment->_parse_table_name($matches[1]);
			$table =& $this->environment->_find_table($table_name_pieces);
			if($table === false)
				return false;
			
			$table_name = $table_name_pieces[2];
			$columns = $table->getColumns();
			$columnNames = array_keys($columns);
			
			$where = null;
			if(isset($matches[2]) && preg_match('/^WHERE\s+((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/i', $matches[2], $first_where))
			{
				$where = $this->buildWhere($first_where[1], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
				if(!$where) {
					return $this->environment->_set_error('Invalid/Unsupported WHERE clause');
				}
			}

			$this->loadQueryClass('delete');
			return new fSQLDeleteQuery($this->environment, $table_name_pieces, $where);
		} else {
			return $this->environment->_set_error('Invalid DELETE query');
		}
	}
	
	function parseDrop($query)
	{
		if(preg_match('/\ADROP(?:\s+(TEMPORARY))?\s+TABLE(?:\s+(IF\s+EXISTS))?\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$temporary = !empty($matches[1]);
			$ifexists = !empty($matches[2]);
			$tables = explode(',', $matches[3]);
			$tableNames = array();
			foreach($tables as $table) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $table, $table_parts)) {
					$tableNames[] = $this->environment->_parse_table_name($table_parts[1]);
				} else {
					return $this->environment->_set_error('Parse error in table listing');
				}
			}
			
			$this->loadQueryClass('dropTable');	
			return new fSQLDropTableQuery($this->environment, $tableNames, $ifexists, false);
		} else if(preg_match('/\ADROP\s+VIEW(?:\s+(IF\s+EXISTS))?\s+(.*)\s*[;]?\Z/is', $query, $matches)) {
			$ifexists = !empty($matches[1]);
			$views = explode(',', $matches[2]);
			$viewNames = array();
			foreach($views as $view) {
				if(preg_match('/(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)/is', $view, $view_parts)) {
					$viewNames[] = $this->environment->_parse_table_name($view_parts[1]);
				} else {
					return $this->environment->_set_error('Parse error in table listing');
				}
			}
			
			$this->loadQueryClass('dropTable');	
			return new fSQLDropTableQuery($this->environment, $viewNames, $ifexists, true);
		
		} else if(preg_match('/\ADROP\s+DATABASE(?:\s+(IF\s+EXISTS))?\s+`?([^\W\d]\w*)`?s*[;]?\Z/is', $query, $matches)) {
			$ifexists = !empty($matches[1]);
			$db_name = $matches[2];
			$this->loadQueryClass('dropDatabase');
			return new fSQLDropDatabaseQuery($this->environment, $db_name, $ifexists);
		} else if(preg_match('/\ADROP\s+SCHEMA(?:\s+(IF\s+EXISTS))?\s+(`?(?:[^\W\d]\w*`?\.`?)?[^\W\d]\w*`?)s*[;]?\Z/is', $query, $matches)) {
			$ifexists = !empty($matches[1]);
			$schema_name_pieces = $this->environment->_parse_schema_name($matches[2]);
			if($schema_name_pieces === false)
				return false;
			$this->loadQueryClass('dropSchema');
			return new fSQLDropSchemaQuery($this->environment, $schema_name_pieces[0], $schema_name_pieces[1], $ifexists);
		
		} else {
			return $this->environment->_set_error('Invalid DROP query');
		}
	}
	
	function parseInsert($query)
	{
		// All INSERT/REPLACE queries are the same until after the table name
		if(preg_match('/\A((INSERT|REPLACE)(?:\s+(IGNORE))?\s+INTO\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?))\s+(.+?)\s*[;]?\Z/is', $query, $matches)) { 
			list(, $beginning, $command, $ignore, $table_name, $the_rest) = $matches;
		} else {
			return $this->environment->_set_error('Invalid INSERT/REPLACE Query');
		}

		// INSERT...SELECT
		if(preg_match('/^(\(.*?\)\s*)?(SELECT\s+.+)/is', $the_rest, $is_matches)) {
			$insert_query = $beginning.' '.$is_matches[1].'VALUES(%s)';
			$id = $this->environment->query($is_matches[2]);
				
			while($values = $this->fetch_row($id)) {
				$values = array_map(array($this, '_prep_for_insert'), $values);
				$full_insert_query = sprintf($insert_query, implode(',', $values));
				$this->environment->query($full_insert_query);
			}
			$this->environment->free_result($id);
			unset ($id, $values);
			return TRUE;
		}
		
		$table_name_pieces = $this->environment->_parse_table_name($table_name);
		$table =& $this->environment->_find_table($table_name_pieces);
		if($table === false) {
			return false;
		} else if($table->isReadLocked()) {
			return $this->environment->_error_table_read_lock($table_name_pieces);
		}

		$tableColumns = $table->getColumns();
		$tableCursor =& $table->getWriteCursor();

		$check_names = 1;
		$replace = !strcasecmp($command, 'REPLACE');
		$ignore = !empty($ignore);
		
		// Column List present and VALUES list
		if(preg_match('/^\(`?(.+?)`?\)\s+VALUES\s*\((.+)\)/is', $the_rest, $matches)) { 
			$Columns = preg_split('/`?\s*,\s*`?/s', $matches[1]);
			$get_data_from = $matches[2];
		}
		// VALUES list but no column list
		else if(preg_match('/^VALUES\s*\((.+)\)/is', $the_rest, $matches)) { 
			$get_data_from = $matches[1];
			$Columns = $table->getColumnNames();
			$check_names = 0;
		}
		// SET syntax
		else if(preg_match('/^SET\s+(.+)/is', $the_rest, $matches)) { 
			$SET = explode(',', $matches[1]);
			$Columns= array();
			$data_values = array();
			
			foreach($SET as $set) {
				list($column, $value) = explode('=', $set);
				$Columns[] = trim($column);
				$data_values[] = trim($value);
			}
			
			$get_data_from = implode(',', $data_values);
		} else {
			return $this->environment->_set_error('Invalid Query');
		}

		preg_match_all("/\s*(DEFAULT|AUTO|NULL|'.*?(?<!\\\\)'|(?:[\+\-]\s*)?\d+(?:\.\d+)?|[^$])\s*(?:$|,)/is", $get_data_from, $newData);
		$dataValues = $newData[1];
	
		if($check_names === 1) {
			if(count($dataValues) != count($Columns)) {
				return $this->environment->_set_error("Number of inserted values and columns not equal");
			}

			$dataValues = array_combine($Columns, $newData[1]);
			$TableColumns = $table->getColumnNames();

			foreach($TableColumns as $col_index => $col_name) {
				if(!in_array($col_name, $Columns)) {
					$Data[$col_index] = "NULL";
				} else {
					$Data[$col_index] = $dataValues[$col_name];
				}
			}

			foreach($Columns as $col_name) {
				if(!in_array($col_name, $TableColumns)) {
					return $this->environment->_set_error("Invalid column name '{$col_name}' found");
				}
			}
		}
		else
		{
			$countData = count($dataValues);
			$countColumns = count($Columns);
			
			if($countData < $countColumns) { 
				$Data = array_pad($dataValues, $countColumns, "NULL");
			} else if($countData > $countColumns) { 
				return $this->environment->_set_error("Trying to insert too many values");
			} else {
				$Data = $dataValues;
			}
		}
		
		$this->loadQueryClass('insert');
		return new fSQLInsertQuery($this->environment, $table_name_pieces, $Columns, $Data, $replace, $ignore);
	}
	
	function parseRollback($query)
	{
		if(preg_match('/\AROLLBACK\s*[;]?\s*\Z/is', $query, $matches)) {
			$this->loadQueryClass('rollback');
			return new fSQLRollbackQuery($this->environment);
		} else {
			return $this->environment->_set_error('Invalid ROLLBACK Query');
		}
	}
	
	function parseSelect($query)
	{
		$matches = array();
		$tables = array();
		$simple = true;
		$distinct = false;
		$isTableless = false;
		if(preg_match('/(.+?)\s+(?:WHERE|(?:GROUP|ORDER)\s+BY|LIMIT)\s+(.+?)/is',$query)) {
			$simple = false;
			preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.+?)\s+FROM\s+(.+?)\s+(?:WHERE|(?:GROUP|ORDER)\s+BY|LIMIT)\s+/is', $query, $matches);
			$matches[4] = preg_replace('/(.+?)\s+(WHERE|(?:GROUP|ORDER)\s+BY|LIMIT)\s+(.+?)/is', '\\1', $matches[4]);
		}
		else if(preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*?)\s+FROM\s+(.+)/is', $query, $matches)) { /* I got the matches, do nothing else */ }
		else { preg_match('/SELECT(?:\s+(ALL|DISTINCT(?:ROW)?))?(\s+RANDOM(?:\((?:\d+)\)?)?\s+|\s+)(.*)/is', $query, $matches); $isTableless = true; }

		$distinct = strncasecmp($matches[1], 'DISTINCT', 8) === 0;
		$has_random = $matches[2] !== ' ';
		
		//expands the tables and loads their data
		$joins = array();
		$joined_info = array( 'tables' => array(), 'offsets' => array(), 'columns' =>array() );
		if(!$isTableless)
		{
			$tbls = explode(',', $matches[4]);
			foreach($tbls as $table_name) {
				if(preg_match('/\A\s*(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(.*)/is', $table_name, $tbl_data)) {
					list(, $table_name, $the_rest) = $tbl_data;
					
					$table_name_pieces = $this->environment->_parse_table_name($table_name);
					$table =& $this->environment->_find_table($table_name_pieces);
					if($table == false)
						return false;
					
					$saveas = $table->getFullName();

					if(preg_match('/\A\s+(?:AS\s+)?([^\W\d]\w*)(.*)/is', $the_rest, $alias_data)) {
						if(!in_array(strtolower($alias_data[1]), array('natural', 'left', 'right', 'full', 'outer', 'cross', 'inner')))
							list(, $saveas, $the_rest) = $alias_data;
					}
				} else {
					return $this->environment->_set_error('Invalid table list');
				}
			
				if(!isset($tables[$saveas]))
					$tables[$saveas] =& $table;
				else
					return $this->environment->_set_error("Table named '$saveas' already specified");

				$joins[$saveas] = array('fullName' => $table_name_pieces, 'joined' => array());
				$table_columns = $table->getColumns();
				$joined_info['tables'][$saveas] = $table_columns;
				$joined_info['offsets'][$saveas] = count($joined_info['columns']);
				$joined_info['columns'] = array_merge($joined_info['columns'], array_keys($table_columns));

				if(!empty($the_rest)) {
					preg_match_all('/((?:(?:NATURAL\s+)?(?:LEFT|RIGHT|FULL)(?:\s+OUTER)?|NATURAL|INNER|CROSS)\s+)?JOIN\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)(?:\s+(?:AS\s+)?([^\W\d]\w*)(?=\s*\Z|\s+(?:USING|ON|INNER|NATURAL|CROSS|LEFT|RIGHT|FULL|JOIN)))?(?:\s+(USING|ON)\s*(?:(?:\((.*?)\))|(?:(?:\()?((?:\S+)\s*=\s*(?:\S+)(?:\))?))))?/is', $the_rest, $join);
					$numJoins = count($join[0]);
					for($i = 0; $i < $numJoins; ++$i) {
						$join_name = strtoupper($join[1][$i]);
						$is_natural = strpos($join_name, 'NATURAL') !== false;
					
						if(strpos($join_name, 'LEFT') !== false)
							$join_type = FSQL_JOIN_LEFT;
						else if(strpos($join_name, 'RIGHT') !== false)
							$join_type = FSQL_JOIN_RIGHT;
						else if(strpos($join_name, 'FULL') !== false)
							$join_type = FSQL_JOIN_FULL;
						else
							$join_type = FSQL_JOIN_INNER;

						$join_table_name_pieces = $this->environment->_parse_table_name($join[2][$i]);
						$join_table =& $this->environment->_find_table($join_table_name_pieces);
						if($join_table === false)
							return false;

						$join_table_name = $join_table->getName();
						$join_table_alias = !empty($join[3][$i]) ? $join[3][$i] : $join_table_name;
						if(!isset($tables[$join_table_alias]))
							$tables[$join_table_alias] = $join_table;
						else
							return $this->environment->_set_error("Table named '$join_table_alias' already specified");
						
						$join_table_columns = $join_table->getColumns();
						$join_table_column_names = array_keys($join_table_columns);

						$clause = strtoupper($join[4][$i]);
						if($clause === 'USING' || (!$clause && $is_natural)) {
							if($clause)   // USING
								$shared_columns = preg_split('/\s*,\s*/', trim($join[5][$i]));
							else  // NATURAL
								$shared_columns = array_intersect($joined_info['columns'], $join_table_column_names);
							
							$conditional = '';
							foreach($shared_columns as $shared_column) {
								$conditional .= " AND {{left}}.$shared_column=$join_table_alias.$shared_column";
							}
							$conditions = substr($conditional, 5);
						}
						else if($clause === 'ON') {
							$conditions = trim($join[6][$i]);
						}

						$joined_info['tables'][$join_table_alias] = $join_table_columns;
						$new_offset = count($joined_info['columns']);
						$joined_info['columns'] = array_merge($joined_info['columns'], $join_table_column_names);

						$conditional = $this->buildWhere($conditions, $joined_info, FSQL_WHERE_ON);
						if(!$conditional) {
							return $this->environment->_set_error('Invalid/Unsupported WHERE clause');
						}
						
						if(!isset($this->environment->join_lambdas[$conditional])) {
							$join_function = create_function('$left_entry,$right_entry', "return $conditional;");
							$this->environment->join_lambdas[$conditional] = $join_function;
						}
						else
							$join_function = $this->environment->join_lambdas[$conditional];

						$joined_info['offsets'][$join_table_alias] = $new_offset;
						
						$joins[$saveas]['joined'][] = array('alias' => $join_table_alias, 'fullName' => $join_table_name_pieces, 'type' => $join_type, 'clause' => $clause, 'comparator' => $join_function);
					}
				}
			}
		}
		
		preg_match_all("/(?:\A|\s*)((?:(?:-?\d+(?:\.\d+)?)|'.*?(?<!\\\\)'|(?:[^\W\d]\w*\s*\(.*?\))|(?:(?:(?:[^\W\d]\w*)\.)?(?:(?:[^\W\d]\w*)|\*)))(?:\s+(?:AS\s+)?[^\W\d]\w*)?)\s*(?:\Z|,)/is", trim($matches[3]), $Columns);
		
		$selectedInfo = array();
		foreach($Columns[1] as $column) {
			// function call	
			if(preg_match('/\A((?:[^\W\d]\w*)\s*\((?:.*?)?\))(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is', $column, $colmatches)) {
				$function_call = $colmatches[1];
				$alias = !empty($colmatches[2]) ? $colmatches[2] : $function_call;
				$selectedInfo[] = array('function', $function_call, $alias);
			}
			// identifier/keyword/column/*
			else if(preg_match('/\A(?:([^\W\d]\w*)\.)?((?:[^\W\d]\w*)|\*)(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is',$column, $colmatches)) {
				list(, $table_name, $column) = $colmatches;
				if($column === '*') {
					if(isset($colmatches[3])) 
						return $this->environment->_set_error('Unexpected alias after "*"');

					$star_tables = !empty($table_name) ? array($table_name) : array_keys($tables);				
					foreach($star_tables as $tname) {
						$start_index = $joined_info['offsets'][$tname];
						$table_columns = $tables[$tname]->getColumns();
						$column_names = array_keys($table_columns);
						foreach($column_names as $index => $column_name) {
							$selectedInfo[] = array('column', $start_index + $index, $column_name, $table_columns[$column_name]);
						}
					}
				} else {
					$alias = !empty($colmatches[3]) ? $colmatches[3] : $column;
					
					if($table_name) {
						$table_columns = $tables[$table_name]->getColumns();
						$column_names = array_keys($table_columns);
						$index = array_search($column, $column_names) + $joined_info['offsets'][$table_name];
						$columnData = $table_columns[$column];
					} else if(strcasecmp($column, "null")){
						$index = array_search($column, $joined_info['columns']);
						$owner_table_name = null;
						foreach($joined_info['tables'] as $join_table_name => $join_table)
						{
							if($index >= $joined_info['offsets'][$join_table_name])
								$owner_table_name = $join_table_name;
							else
								break;
						}
						$columnData = $joined_info['tables'][$owner_table_name][$column];
					}
					else  // "null" keyword
					{
						$selectedInfo[] = array('null', 'null', $alias, 
												array('type'=>FSQL_TYPE_STRING,'null'=>true,'default'=>null,'auto'=>false,'key'=>'n','restraint'=>array())
											);
						continue;
					}
						
					$selectedInfo[] = array('column', $index, $alias, $columnData);
				}
			}
			// numeric constant
			else if(preg_match("/\A(-?\d+(?:\.\d+)?)(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is", $column, $colmatches)) {
				$value = $colmatches[1];
				$alias = !empty($colmatches[2]) ? $colmatches[2] : $value;
				$number_type = strpos($value, '.') === false ? FSQL_TYPE_INTEGER : FSQL_TYPE_FLOAT;
				$selectedInfo[] = array('number', $value, $alias,
										array('type'=>$number_type,'null'=>false,'default'=>'','auto'=>false,'key'=>'n','restraint'=>array())
									);
			}
			// string constant
			else if(preg_match("/\A('(.*?(?<!\\\\))')(?:\s+(?:AS\s+)?([^\W\d]\w*))?\Z/is", $column, $colmatches)) {
				$value = $colmatches[2];
				$alias = !empty($colmatches[3]) ? $colmatches[3] : $value;
				$selectedInfo[] = array('string', $colmatches[1], $alias,
										array('type'=>FSQL_TYPE_STRING,'null'=>false,'default'=>'','auto'=>false,'key'=>'n','restraint'=>array())
									);
			}
			else {
				return $this->environment->_set_error("Parse Error: Unknown value in SELECT clause: ". $column);
			}
		}
		
		$where = null;
		$group_list = null;
		$having = null;
		$orderby = null;
		$limit = null;
		
		if(!$simple)
		{
			if(preg_match('/\s+LIMIT\s+(?:(?:(\d+)\s*,\s*(\-1|\d+))|(\d+))/is', $query, $additional)) {
				list(, $limit_start, $limit_stop) = $additional;
				if($additional[3]) { $limit_stop = $additional[3]; $limit_start = 0; }
				else if($additional[2] != -1) { $limit_stop += $limit_start; }
				$limit = array($limit_start, $limit_stop);
			}
			
			if(preg_match('/\s+ORDER\s+BY\s+(?:(.*)\s+LIMIT|(.*))?/is', $query, $additional)) {
				if(!empty($additional[1])) { $ORDERBY = explode(',', $additional[1]); }
				else { $ORDERBY = explode(',', $additional[2]); }
				for($i = 0; $i < count($ORDERBY); ++$i) {
					if(preg_match('/([^\W\d]\w*)(?:\s+(ASC|DESC))?/is', $ORDERBY[$i], $additional)) {
						$index = array_search($additional[1], $joined_info['columns']);
						if(empty($additional[2])) { $additional[2] = 'ASC'; }
						$tosort[] = array('key' => $index, 'ascend' => !strcasecmp("ASC", $additional[2]));
					}
				}
				$orderby = new fSQLOrderByClause($tosort);
			}

			if(preg_match('/\s+GROUP\s+BY\s+(?:(.*)\s+(?:HAVING|ORDER\s+BY|LIMIT)|(.*))?/is', $query, $additional)) {
				$group_clause = !empty($additional[1]) ? $additional[1] : $additional[2];
				$GROUPBY = explode(',', $group_clause);
				foreach($GROUPBY as $group_item)
				{
					if(preg_match('/([^\W\d]\w*)(?:\s+(ASC|DESC))?/is', $group_item, $additional)) {
						$index = array_search($additional[1], $joined_info['columns']);
						if(empty($additional[2])) { $additional[2] = 'ASC'; }
						$group_list[] = array('key' => $index, 'ascend' => !strcasecmp("ASC", $additional[2]));
					}
				}
			}
			
			if(preg_match('/\s+HAVING\s+((?:.+)(?:(?:((?:\s+)(?:AND|OR)(?:\s+))?(?:.+)?)*)?)(?:\s+(?:ORDER\s+BY|LIMIT))?/is', $query, $additional)) {
				$having = $this->buildWhere($additional[1], $joined_info, FSQL_WHERE_HAVING);
				if(!$having) {
					return $this->environment->_set_error('Invalid/Unsupported HAVING clause');
				}
			}
			
			if(preg_match('/\s+WHERE\s+((?:.+)(?:(?:((?:\s+)(?:AND|OR)(?:\s+))?(?:.+)?)*)?)(?:\s+(?:(?:GROUP|ORDER)\s+BY|LIMIT))?/is', $query, $first_where)) {
				$where = $this->buildWhere($first_where[1], $joined_info);
				if(!$where) {
					return $this->_set_error('Invalid/Unsupported WHERE clause');
				}
			}
		}
		
		$this->loadQueryClass('select');
		return new fSQLSelectQuery($this->environment, $selectedInfo, $joins, $where, $group_list, $having, $orderby, $limit, $distinct);
	}
	
	function parseStart($query)
	{
		if(preg_match('/\ASTART\s+TRANSACTION\s*[;]?\Z/is', $query, $matches)) {			
			$this->loadQueryClass('start');
			return new fSQLStartQuery($this->environment);
		} else {
			return $this->_set_error('Invalid Query');
		}
	}
	
	function parseUpdate($query)
	{
		if(preg_match('/\AUPDATE(?:\s+(IGNORE))?\s+(`?(?:[^\W\d]\w*`?\.`?){0,2}[^\W\d]\w*`?)\s+SET\s+(.*)(?:\s+WHERE\s+.+)?\s*[;]?\Z/is', $query, $matches)) {
			list(, $ignore, $table_name, $set_clause) = $matches;
			$ignore = !empty($ignore);
			$set_clause = preg_replace('/(.+?)(\s+WHERE\s+)(.*)/is', '\\1', $set_clause);

			$table_name_pieces = $this->environment->_parse_table_name($table_name);
			$table =& $this->environment->_find_table($table_name_pieces);
			if($table === false) {
				return false;
			}
		
			$columns = $table->getColumns();
			$columnNames = array_keys($columns);

			if(preg_match_all("/`?((?:\S+)`?\s*=\s*(?:'(?:.*?)'|\S+))`?\s*(?:,|\Z)/is", $set_clause, $sets)) {
				foreach($sets[1] as $set) {
					$s = preg_split('/`?\s*=\s*`?/', $set);
					$SET[] = $s;
					if(!isset($columns[$s[0]])) {
						return $this->environment->_set_error("Invalid column name '{$s[0]}' found");
					}
				}
			}
			else
				$SET[0] =  preg_split('/\s*=\s*/', $set_clause);

			$where = null;
			if(preg_match('/\s+WHERE\s+((?:.+)(?:(?:(?:\s+(AND|OR)\s+)?(?:.+)?)*)?)/is', $query, $sets))
			{
				$where = $this->buildWhere($sets[1], array('tables' => array($table_name => $columns), 'offsets' => array($table_name => 0), 'columns' => $columnNames));
				if(!$where) {
					return $this->environment->_set_error('Invalid/Unsupported WHERE clause');
				}
			}
			
			$this->loadQueryClass('update');
			return new fSQLUpdateQuery($this->environment, $table_name_pieces, $SET, $where, $ignore);
		} else {
			return $this->environment->_set_error('Invalid UPDATE query');
		}
	}
}

?>