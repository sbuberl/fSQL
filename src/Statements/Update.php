<?php

namespace FSQL\Statements;

use FSQL\Environment;

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
        $cursor = $table->getCursor();

        $uniqueKeyColumns = [];
        foreach ($columns as $column => $columnDef) {
            if ($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
                $uniqueKeyColumns[] = $colIndices[$column];
            }
        }

        $updatedKeyColumns = array_intersect(array_keys($this->updates), $uniqueKeyColumns);
        $keyLookup = [];
        if (!empty($updatedKeyColumns)) {
            foreach ($cursor as $rowId => $entry) {
                foreach ($updatedKeyColumns as $unique) {
                    if (!isset($keyLookup[$unique])) {
                        $keyLookup[$unique] = array();
                    }

                    $keyLookup[$unique][$entry[$unique]] = $rowId;
                }
            }
        }

        $updates = 'array('.implode(',', $this->updates).')';
        $line = "\t\t\$table->updateRow(\$rowId, \$updates);\r\n";
        $line .= "\t\t\$this->affected++;\r\n";

        // find all updated columns that are part of a unique key
        // if there are any, call checkUnique to validate still unique.
        $code = '';
        if (!empty($updatedKeyColumns)) {
            $code = <<<EOV
\$violation = \$this->whereKeyCheck(\$rowId, \$entry, \$keyLookup, \$updatedKeyColumns);
if(\$violation) {
    if(!\$this->ignore) {
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

    private function whereKeyCheck($rowId, $entry, &$keyLookup, $uniqueColumns)
    {
        foreach ($uniqueColumns as $unique) {
            $currentLookup = &$keyLookup[$unique];
            $currentVal = $entry[$unique];
            if (isset($currentLookup[$currentVal])) {
                if ($currentLookup[$currentVal] != $rowId) {
                    return true;
                }
            } else {
                $currentLookup[$currentVal] = $rowId;
            }
        }

        return false;
    }

}
