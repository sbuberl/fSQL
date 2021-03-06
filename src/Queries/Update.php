<?php

namespace FSQL\Queries;

use FSQL\Environment;
use FSQL\Database\Key;
use FSQL\Database\TableCursor;

class Update extends DataModifyQuery
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

        $updates = [];
        foreach ($this->updates as $colIndex => $newValue) {
            if (is_string($newValue)) {
                $newValue = "'".$newValue."'";
            }
            $updates[$colIndex] = "$colIndex => $newValue";
        }
        $updates = 'array('.implode(',', $updates).')';
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

    private function checkUnique(array $uniqueKeys, $rowId, array $entry)
    {
        $entry = array_replace($entry, $this->updates);

        foreach($uniqueKeys as $key) {
            $toFind = $key->extractIndex($entry);
            $foundRowId = $key->lookup($toFind);
            if($foundRowId !== false && $foundRowId !== $rowId) {
                return false;
            }
        }
        return true;
    }
}
