<?php

namespace FSQL\Statements;

use FSQL\Environment;
use FSQL\Database\Key;

class Insert extends DataModifyStatement
{
    const REPLACE = 2;
    const IGNORE = 1;
    const ERROR = 0;

    private $insert_id = null;
    private $tableFullName;
    private $data;
    private $mode;

    public function __construct(Environment $environment, array $fullName, array $data, $mode)
    {
        parent::__construct($environment);
        $this->tableFullName = $fullName;
        $this->data = $data;
        $this->mode = $mode;
    }

    public function insertId()
    {
        return $this->insert_id;
    }

    public function execute()
    {
        $this->affected = 0;

        $newRows = [];
        $keyColumns = [];

        $table = $this->environment->find_table($this->tableFullName);
        if(!$table)
            return false;

        $tableColumns = $table->getColumns();
        $tableCursor = $table->getWriteCursor();
        $keys = $table->getKeys();

        $keyColumns = [];
        foreach($keys as $key) {
            if($key->type() & Key::UNIQUE) {
                $keyColumns = array_merge($keyColumns, $key->columns());
            }
        }

        foreach ($this->data as $row) {
            $newEntry = [];

            ////Load Columns & Data for the Table
            $colIndex = 0;
            foreach ($tableColumns as $columnName => $columnDef) {
                $data = trim($row[$columnName]);
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

                ++$colIndex;
            }

            $newRows[] = $newEntry;
        }

        foreach ($newRows as $newRow) {
            foreach($keys as $key) {
                if($key->type() & Key::UNIQUE) {
                    $indexValue = $key->extractIndex($newRow);
                    if($indexValue !== false) {
                        $rowid = $key->lookup($indexValue);
                        if($rowid !== false) {
                            if($this->mode === self::REPLACE) {
                            // may have already ben deleted so check return
                                if($tableCursor->seek($rowid) !== false) {
                                    $tableCursor->deleteRow();
                                    $this->affected++;
                                }
                            }
                            elseif($this->mode !== self::IGNORE) {
                                return $this->environment->set_error("Duplicate value found on key");
                            } else {
                                return true;
                            }
                        }
                    }
                }
            }

            $tableCursor->appendRow($newRow);

            ++$this->affected;
        }

        $this->commit($table);

        return true;
    }
}
