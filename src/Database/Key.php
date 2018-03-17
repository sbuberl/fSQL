<?php

namespace FSQL\Database;

abstract class Key
{
    const NONE = 0;
    const NULLABLE = 1;
    const NON_UNIQUE = 2;
    const UNIQUE = 4;
    const PRIMARY = 12;

    abstract public function name();

    abstract public function type();

    abstract public function columns();

    abstract public function addEntry($rowId, $values);

    abstract public function deleteEntry($rowId);

    abstract public function lookup($key);

    abstract public function reset();

	/**
	 * Given a row from this key's table extract the key data
	 * for use as a parameter to lookup().
	 *
	 * @param array $row
	 */
	public function extractIndex(array $row)
	{
		$columns = $this->columns();
		if($columns)
		{
			switch(count($columns))
			{
				case 1:
					return $row[$columns[0]];
				case 2:
					return [$row[$columns[0]], $row[$columns[1]]];
				case 3:
					return [$row[$columns[0]], $row[$columns[1]], $row[$columns[2]]];
				default:
					// ugly but it works.  last resort
					return array_intersect_key($row, array_flip($columns));
			}
		}
		else
			return false;
	}

	public function updateEntry($rowId, $values)
	{
		return $this->deleteEntry($rowId) && $this->addEntry($rowId, $values);
	}
}