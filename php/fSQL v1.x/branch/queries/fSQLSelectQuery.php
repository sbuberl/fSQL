<?php

class fSQLSelectQuery extends fSQLQuery
{
	var $selectedInfo = null;
	
	var $joins = null;
	
	var $where = null;
	
	var $groupList = null;
	
	var $having = null;
	
	var $orderby = null;
	
	var $limit = null;
	
	var $distinct = false;
	
	function fSQLSelectQuery(&$environment, $selectedInfo, $joins, $where, $groupList, $having, $orderby, $limit, $distinct)
	{
		parent::fSQLQuery($environment);
		$this->selectedInfo = $selectedInfo;
		$this->joins = $joins;
		$this->where = $where;
		$this->groupList = $groupList;
		$this->having = $having;
		$this->orderby = $orderby;
		$this->limit = $limit;
		$this->distinct = $distinct;
	}
	
	////Select data from the DB
	function execute()
	{	
		// Loads and joins the tables into single data set
		$data = array();
		$joined_info = array( 'tables' => array(), 'offsets' => array(), 'columns' =>array() );
		foreach($this->joins as $saveas => $join) {
			$base_table =& $this->environment->_find_table($join['fullName']);
			if($base_table === false)
				return false;
			$base_table_columns = $base_table->getColumns();
			$join_columns_size = count($base_table->getColumnNames());
			$join_data = $base_table->getEntries();
			
			$joined_info['tables'][$saveas] = $base_table_columns;
			$joined_info['offsets'][$saveas] = count($joined_info['columns']);
			$joined_info['columns'] = array_merge($joined_info['columns'], array_keys($base_table_columns));
			
			foreach($join['joined'] as $join_op) {
				$joining_table =& $this->environment->_find_table($join_op['fullName']);
				if($joining_table === false)
					return false;
				$joining_table_columns = $joining_table->getColumns();
				$joining_columns_size = count($joining_table_columns);
				
				$join_table_alias = $join_op['alias'];
				$joined_info['tables'][$join_table_alias] = $joining_table_columns;
				$joined_info['offsets'][$join_table_alias] = count($joined_info['columns']);
				$joined_info['columns'] = array_merge($joined_info['columns'], array_keys($joining_table_columns));
				
				switch($join_op['type'])
				{
					default:
						$join_data = $this->_inner_join($join_data, $joining_table->getEntries(), $join_op['comparator']);
						break;
					case FSQL_JOIN_LEFT:
						$join_data = $this->_left_join($join_data, $joining_table->getEntries(), $join_op['comparator'], $joining_columns_size);
						break;
					case FSQL_JOIN_RIGHT:
						$join_data = $this->_right_join($join_data, $joining_table->getEntries(), $join_op['comparator'], $join_columns_size);
						break;
					case FSQL_JOIN_FULL:
						$join_data = $this->_full_join($join_data, $joining_table->getEntries(), $join_op['comparator'], $join_columns_size, $joining_columns_size);
						break;
				}

				$join_columns_size += $joining_columns_size;
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
				}
				else
					$data = $join_data;
			}
		}
		
		$fullColumnsInfo = array();
		$group_key = NULL;
		$final_code = NULL;
		if(!empty($this->groupList))
		{
			$joined_info['group_columns'] = array();
			
			if(count($this->groupList) === 1)
			{
				$group_col = $this->groupList[0]['key'];
				$group_key = '$entry[' . $group_col .']';
				$group_array = array($group_key);
				$joined_info['group_columns'][] = $group_col;
			}
			else
			{
				$all_ascend = 1;
				$group_array = array();
				$group_key_list = '';
				foreach($this->groupList as $group_item)
				{
					$all_ascend &= (int) $group_item['ascend'];
					$group_col = $group_item['key'];
					$group_array[] = $group_col;
					$group_key_list .= '$entry[' . $group_col .'], ';
					$joined_info['group_columns'][] = $group_col;
				}
				$group_key = 'serialize(array('. substr($group_key_list, 0, -2) . '))';
			}
			
			$select_line = "";
			foreach($this->selectedInfo as $info) {
				list($select_type, $select_value, $select_alias) = $info;
				$column_info = null;
				switch($select_type) {
					case 'column':
						if(!in_array($select_value, $group_array)) {
							return $this->environment->_set_error("Selected column '{$joined_info['columns'][$select_value]}' is not a grouped column");
						}
						$select_line .= "\$group[0][$select_value], ";
						$column_info = $info[3];
						break;
					case 'number':
					case 'string':
						$select_line .= $select_value.', ';
						$column_info = $info[3];
						break;
					case 'function':
						$expr = $this->environment->parser->buildExpression($select_value, $joined_info);
						$select_line .= $expr['expression'].', ';
						$column_info = $expr['type'];
						break;
				}
				$column_info['name'] = $select_alias;
				$fullColumnsInfo[] = $column_info;
			}
			
			$line = '$grouped_set['.$group_key.'][] = $entry;';
			$final_line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
			$grouped_set = array();
			
			if($this->having !== null) {
				$final_line = "if({$this->having}) {\r\n\t\t\t\t\t$final_line\r\n\t\t\t\t}";
			}
			
			$final_code = <<<EOT
			foreach(\$grouped_set as \$group) {
				$final_line
			}
EOT;
		}
		else
		{
			$select_line = "";
			foreach($this->selectedInfo as $info) {
				list($select_type, $select_value, $select_alias) = $info;
				$column_info = null;
				switch($select_type) {
					case 'column':
						$select_line .= "\$entry[$select_value], ";
						$column_info = $info[3];
						break;
					case 'null':
					case 'number':
					case 'string':
						$select_line .= $select_value.', ';
						$column_info = $info[3];
						break;
					case 'function':
						$expr = $this->environment->parser->buildExpression($select_value, $joined_info, false);
						$select_line .= $expr['expression'].', ';
						$column_info = $expr['type'];
						break;
				}
				$column_info['name'] = $select_alias;
				$fullColumnsInfo[] = $column_info;
			}
			$line = '$final_set[] = array('. substr($select_line, 0, -2) . ');';
			$group = $data;
		}
		
		if($this->joins !== null) {
			if($this->where !== null)
				$line = "if({$this->where}) {\r\n\t\t\t\t\t$line\r\n\t\t\t\t}";
				
			$code = <<<EOT
			foreach(\$data as \$entry) {
				$line
			}
				
$final_code
EOT;
		}
		else
			$code = $line;
		
		$final_set = array();
		eval($code);
					
		// Execute an ORDER BY
		if(!empty($this->orderby))
		{
			$this->orderby->sort($final_set);
		}
		
		// Execute a LIMIT
		if($this->limit !== null)
			$final_set = array_slice($final_set, $this->limit[0], $this->limit[1]);
		
		return $this->environment->_create_result_set($fullColumnsInfo, $final_set);
	}
	
	function _cross_product($left_data, $right_data)
	{
		if(empty($left_data) || empty($right_data))
			return array();

		$new_join_data = array();

		foreach($left_data as $left_entry)
		{
			foreach($right_data as $right_entry) {
				$new_join_data[] = array_merge($left_entry, $right_entry);
			}
		}

		return $new_join_data;
	}

	function _inner_join($left_data, $right_data, $join_comparator)
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

	function _left_join($left_data, $right_data, $join_comparator, $pad_length)
	{
		$new_join_data = array();
		$right_padding = array_fill(0, $pad_length, NULL);

		foreach($left_data as $left_entry)
		{
			$match_found = false;
			foreach($right_data as $right_entry) {
				if($join_comparator($left_entry, $right_entry)) {
					$match_found = true;
					$new_join_data[] = array_merge($left_entry, $right_entry);
				}
			}

			if(!$match_found) 
				$new_join_data[] = array_merge($left_entry, $right_padding);
		}

		return $new_join_data;
	}

	function _right_join($left_data, $right_data, $join_comparator, $pad_length)
	{
		$new_join_data = array();
		$left_padding = array_fill(0, $pad_length, NULL);

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

	function _full_join($left_data, $right_data, $join_comparator, $left_pad_length, $right_pad_length)
	{
		$new_join_data = array();
		$matched_rids = array();
		$left_padding = array_fill(0, $left_pad_length, NULL);
		$right_padding = array_fill(0, $right_pad_length, NULL);

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
}

?>