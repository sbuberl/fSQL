<?php

namespace FSQL\Statements;

use FSQL\Environment;
use FSQL\Database\Key;

class Update extends DataModifyStatement
{
    const IGNORE = 1;
    const ERROR = 0;

    private $tableFullName;
    private $updates;
    private $where;
    private $ignore;

    public function __construct(Environment $environment, array $fullName, array $updates, $where, $ignore)
    {
        parent::__construct($environment);
        $this->tableFullName = $fullName;
        $this->updates = $updates;
        $this->where = $where;
        $this->ignore = $ignore;
    }

    public function execute()
    {
        $this->affected = 0;

        $keyColumns = [];

        $table = $this->environment->find_table($this->tableFullName);
        if(!$table)
            return false;

        $columns = $table->getColumns();
        $columnNames = array_keys($columns);
        $colIndices = array_flip($columnNames);
        $cursor = $table->getWriteCursor();

        // find all unique keys and columns to watch.
        $keys = $table->getKeys();
        $uniqueKeys = [];
        $uniqueKeyColumns = [];
        foreach($keys as $key) {
            if($key->type() & Key::UNIQUE) {
                $uniqueKeyColumns = array_merge($uniqueKeyColumns, $key->columns());
                $uniqueKeys[] = $key;
            }
        }

        $updatedKeyColumns = array_intersect(array_keys($this->updates), $uniqueKeyColumns);

        $updates = 'array('.implode(',', $this->updates).')';
        $code = '';

        // find all updated columns that are part of a unique key
        // if there are any, call checkUnique to validate still unique.
        if (!empty($updatedKeyColumns)) {
            $code .= "\t\tif(\$this->checkUnique(\$uniqueKeys, \$rowId, \$entry) === false) {\r\n";
            $code .= "\t\t\treturn (\$this->ignore) ? true : \$this->environment->set_error('Duplicate values found in unique key during UPDATE');\r\n";
            $code .= "\t\t}\r\n";
        }

        $code .= "\t\t\$cursor->updateRow(\$updates);\r\n";
        $code .= "\t\t\$this->affected++;\r\n";

        if ($this->where) {
            $code = "\tif({$this->where}) {\r\n$code\r\n\t}";
        }

        $updateCode = <<<EOC
foreach( \$cursor as \$rowId => \$entry)
{
\$updates = $updates;
$code
}
return true;
EOC;
        $success = eval($updateCode);
        if (!$success) {
            return $success;
        }

        $this->commit($table);

        return true;
    }

    private function checkUnique($uniqueKeys, $rowId, $updatedRow)
    {
        foreach($uniqueKeys as $key) {
            $newValue = $key->extractIndex($updatedRow);
            $foundRowId = $key->lookup($newValue);
            if($foundRowId !== false && $foundRowId !== $rowId) {
                return false;
            }
        }
        return true;
    }
}
