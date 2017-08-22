<?php

namespace FSQL\Statements;

use FSQL\Environment;

class Insert extends DataModifyStatement
{
    private $insert_id = null;
    private $tableFullName;
    private $data;
    private $ignore;
    private $replace;

    public function __construct(Environment $environment, array $fullName, array $data, $ignore, $replace)
    {
        parent::__construct($environment);
        $this->tableFullName = $fullName;
        $this->data = $data;
        $this->ignore = $ignore;
        $this->replace = $replace;
    }

    public function execute()
    {
        $this->affected = 0;

        $newEntry = [];
        $keyColumns = [];

        $table = $this->environment->find_table($this->tableFullName);
        if(!$table)
            return false;

        $tableColumns = $table->getColumns();
        $tableCursor = $table->getCursor();

        ////Load Columns & Data for the Table
        $colIndex = 0;
        foreach ($tableColumns as $columnName => $columnDef) {
            $data = trim($this->data[$columnName]);
            $data = strtr($data, array('$' => '$', '$' => '\\$'));

            ////Check for Auto_Increment/Identity
            if ($columnDef['auto'] == 1) {
                $identity = $table->getIdentity();
                if (empty($data) || !strcasecmp($data, 'AUTO') || !strcasecmp($data, 'NULL') || !strcasecmp($data, 'DEFAULT')) {
                    $insert_id = $identity->nextValueFor();
                    if ($insert_id !== false) {
                        $this->insert_id = $insert_id;
                        $newEntry[$colIndex] = $this->insert_id;
                    } else {
                        return $this->environment->set_error('Error getting next value for identity column: '.$columnName);
                    }
                } else {
                    if ($identity->getAlways()) {
                        return $this->environment->set_error('Manual value inserted into an ALWAYS identity column');
                    }
                    $data = $this->environment->parse_value($columnDef, $data);
                    if ($data === false) {
                        return false;
                    }
                    $newEntry[$colIndex] = $data;
                }
            }
            ///Check for NULL Values
            elseif ((!strcasecmp($data, 'NULL') && !$columnDef['null']) || empty($data) || !strcasecmp($data, 'DEFAULT')) {
                $data = $this->environment->parse_value($columnDef, $columnDef['default']);
                if ($data === false) {
                    return false;
                }
                $newEntry[$colIndex] = $data;
            } else {
                $data = $this->environment->parse_value($columnDef, $data);
                if ($data === false) {
                    return false;
                }
                $newEntry[$colIndex] = $data;
            }

            ////See if it is a PRIMARY KEY or UNIQUE
            if ($columnDef['key'] == 'p' || $columnDef['key'] == 'u') {
                $keyColumns[$colIndex] = $columnName;
            }

            ++$colIndex;
        }

        foreach ($keyColumns as $colIndex => $columnName) {
            if ($this->replace) {
                $delete = array();
                $tableCursor->rewind();
                while ($tableCursor->valid()) {
                    $row = $tableCursor->current();
                    if ($row[$colIndex] == $newEntry[$colIndex]) {
                        $delete[] = $tableCursor->key();
                    }
                    $tableCursor->next();
                }
                if (!empty($delete)) {
                    foreach ($delete as $d) {
                        ++$this->affected;
                        $table->deleteRow($d);
                    }
                }
            } else {
                $tableCursor->rewind();
                while ($tableCursor->valid()) {
                    $row = $tableCursor->current();
                    if ($row[$colIndex] == $newEntry[$colIndex]) {
                        if (!$this->ignore) {
                            return $this->environment->set_error("Duplicate value for unique column '{$columnName}'");
                        } else {
                            return true;
                        }
                    }
                    $tableCursor->next();
                }
            }
        }

        $table->insertRow($newEntry);

        ++$this->affected;

        $this->commit($table);

        return true;
    }
}
