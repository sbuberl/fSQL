<?php

namespace FSQL\Database;

use FSQL\File;
use FSQL\LockableFile;
use FSQL\MicrotimeLockFile;
use FSQL\Types;

class CachedTable extends Table
{
    public $columnsLockFile;
    public $columnsFile;
    public $dataLockFile;
    public $dataFile;
    private $lock = null;

    public function __construct(Schema $schema, $table_name)
    {
        parent::__construct($schema, $table_name);
        $path_to_schema = $this->schema->path();
        $columns_path = $path_to_schema.$table_name.'.columns';
        $data_path = $path_to_schema.$table_name.'.data';
        $this->columnsLockFile = new MicrotimeLockFile(new File($columns_path.'.lock.cgi'));
        $this->columnsFile = new LockableFile(new File($columns_path.'.cgi'));
        $this->dataLockFile = new MicrotimeLockFile(new File($data_path.'.lock.cgi'));
        $this->dataFile = new LockableFile(new File($data_path.'.cgi'));
    }

    public static function create(Schema $schema, $table_name, array $columnDefs)
    {
        $table = new static($schema, $table_name);
        $table->columns = $columnDefs;

        // create the columns lock
        $table->columnsLockFile->write();
        $table->columnsLockFile->reset();

        // create the columns file
        $table->columnsFile->acquireWrite();
        $toprint = $table->printColumns($columnDefs);
        fwrite($table->columnsFile->getHandle(), $toprint);
        $table->columnsFile->releaseWrite();

        // create the data lock
        $table->dataLockFile->write();
        $table->dataLockFile->reset();

        // create the data file
        $table->dataFile->acquireWrite();
        fwrite($table->dataFile->getHandle(), "0\r\n");
        $table->dataFile->releaseWrite();

        return $table;
    }

    private function printColumns(array $columnDefs)
    {
        $toprint = count($columnDefs)."\r\n";
        foreach ($columnDefs as $name => $column) {
            $default = $column['default'];
            $type = $column['type'];
            $auto = $column['auto'];
            if (is_string($default) && $default !== 'NULL') {
                $default = "'$default'";
            } elseif ($default === null) {
                $default = 'NULL';
            }

            $restraint = '';
            if ($type === Types::ENUM) {
                $restraint = "'".implode("','", $column['restraint'])."'";
            } elseif ($auto) {
                $restraint = implode(',', $column['restraint']);
            }

            if(empty($column['key'])) {
                $key = 'n';
                $this->columns[$name]['key'] = 'n';
            } else {
                $key = $column['key'];
            }
            $toprint .= $name.': '.$type.';'.$restraint.';'.$auto.';'.$default.';'.$key.';'.$column['null'].";\r\n";
        }

        return $toprint;
    }

    public function exists()
    {
        return $this->columnsFile->exists();
    }

    public function temporary()
    {
        return false;
    }

    public function drop()
    {
        $this->columnsFile->drop();
        $this->columnsLockFile->drop();
        $this->dataFile->drop();
        $this->dataLockFile->drop();
    }

    public function truncate()
    {
        $this->dataLockFile->acquireWrite();
        $this->dataLockFile->write();

        $this->dataFile->acquireWrite();
        $dataFile = $this->dataFile->getHandle();
        ftruncate($dataFile, 0);
        fwrite($dataFile, "0\r\n");
        $this->dataFile->releaseWrite();
        $this->dataLockFile->releaseWrite();

        $this->entries = array();
    }

    public function getColumns()
    {
        $this->columnsLockFile->acquireRead();
        if ($this->columnsLockFile->wasModified()) {
            $this->columnsLockFile->accept();

            $this->columnsFile->acquireRead();
            $columnsHandle = $this->columnsFile->getHandle();

            fseek($columnsHandle, 0, SEEK_SET);

            $line = fgets($columnsHandle);
            if (!preg_match("/^(\d+)/", $line, $matches)) {
                $this->columnsFile->releaseRead();
                $this->columnsLockFile->releaseRead();
                return false;
            }

            $num_columns = $matches[1];
            $keys = [];

            for ($i = 0; $i < $num_columns; ++$i) {
                $line = fgets($columnsHandle);
                if (preg_match("/(\S+): (dt|d|i|f|s|t|e);(.*);(0|1);(-?\d+(?:\.\d+)?|'.*'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
                    $name = $matches[1];
                    $type = $matches[2];
                    $restraintString = $matches[3];
                    $auto = (int) $matches[4];
                    $default = $matches[5];
                    $key = $matches[6];
                    $null = (int) $matches[7];

                    if ($type === Types::INTEGER || $type === Types::ENUM) {
                        $default = (int) $default;
                    } elseif ($type === Types::FLOAT) {
                        $default = (float) $default;
                    } elseif ($default{0} == "'" && substr($default, -1) == "'") {
                        $default = substr($default, 1, -1);
                    } elseif ($default === 'NULL') {
                        $default = null;
                    }

                    if ($auto === 1 && !empty($restraintString)) {
                        list($current, $always, $start, $increment, $min, $max, $cycle) = explode(',', $restraintString);
                        $restraint = [(int) $current, (int) $always, (int) $start, (int) $increment, (int) $min, (int) $max, (int) $cycle];
                    } elseif ($type === Types::ENUM && preg_match_all("/'(.*?(?<!\\\\))'/", $restraintString, $enumMatches) !== false) {
                        $restraint = $enumMatches[1];
                    } else {
                        $restraint = [];
                    }

                    if($key === 'p') {
                        $key_name = $this->name.'_pk';
                        $key_type = Key::PRIMARY;
                        if(!isset($keys[$key_name]))
                            $keys[$key_name] = ['type' => $key_type, 'columns' => [$i]];
                        else   // add a column
                            $keys[$key_name]['columns'][] = $i;
                    }

                    $this->columns[$name] = [
                        'type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $key, 'null' => $null, 'restraint' => $restraint,
                    ];
                } else {
                    $this->columnsFile->releaseRead();
                    $this->columnsLockFile->releaseRead();
                    return false;
                }
            }

            foreach($keys as $name => $key) {
                unset($this->keys[$name]);
                $this->keys[$name] = new MemoryKey($name, $key['type'], $key['columns']);
            }

            $this->columnsFile->releaseRead();
        }

        $this->columnsLockFile->releaseRead();

        return parent::getColumns();
    }

    public function getEntries()
    {
        $this->loadEntries();

        return parent::getEntries();
    }

    public function getCursor()
    {
        $this->loadEntries();

        return parent::getCursor();
    }

    public function newCursor()
    {
        $this->loadEntries();

        return parent::newCursor();
    }

    public function getWriteCursor()
    {
        $this->loadEntries();

        return parent::getWriteCursor();
    }

    private function loadEntries()
    {
        $this->dataLockFile->acquireRead();
        if ($this->dataLockFile->wasModified()) {
            $this->dataLockFile->accept();

            $entries = array();
            $this->dataFile->acquireRead();
            $dataHandle = $this->dataFile->getHandle();

            fseek($dataHandle, 0, SEEK_SET);
            $line = fgets($dataHandle);
            if (!preg_match("/^(\d+)/", $line, $matches)) {
                $this->dataFile->releaseRead();
                $this->dataLockFile->releaseRead();

                return false;
            }

            $num_entries = rtrim($matches[1]);
            $keys = $this->getKeys();
            foreach($keys as $key)
            {
                $key->reset();
            }

            if ($num_entries != 0) {
                $columnDefs = array_values($this->getColumns());
                for ($i = 0; $i < $num_entries; ++$i) {
                    $line = rtrim(fgets($dataHandle));

                    if (preg_match("/^(\d+):(.*)$/", $line, $matches)) {
                        $row = $matches[1];
                        $data = trim($matches[2]);
                    } else {
                        $this->dataFile->releaseRead();
                        $this->dataLockFile->releaseRead();
                        return false;
                    }

                    preg_match_all("#((-?\d+(?:\.\d+)?)|'(.*?(?<!\\\\))'|NULL);#s", $data, $matches);
                    $numMatches = count($matches[0]);
                    for ($m = 0; $m < $numMatches; ++$m) {
                        if ($matches[1][$m] === 'NULL') {
                            $entries[$row][$m] = null;
                        } elseif (!empty($matches[2][$m])) {
                            $number = $matches[2][$m];
                            if (strpos($number, '.') !== false) {
                                $number = (float) $number;
                            } else {
                                $number = (int) $number;
                            }
                            $entries[$row][$m] = $number;
                        } elseif ($columnDefs[$m]['type'] === Types::ENUM) {
                            $index = (int) $matches[2][$m];
                            $entries[$row][$m] = $index > 0 ? $columnDefs[$m]['restraint'][$index - 1] : '';
                        } else {
                            $entries[$row][$m] = $matches[3][$m];
                        }
                    }

                    foreach($keys as $key) {
                        $idx = $key->extractIndex($entries[$row]);
                        $key->addEntry($row, $idx);
                    }
                }
            }

            $this->entries = $entries;

            $this->dataFile->releaseRead();
        }

        $this->dataLockFile->releaseRead();

        return true;
    }

    public function setColumns(array $columnDefs)
    {
        $this->columnsLockFile->acquireWrite();

        parent::setColumns($columnDefs);

        $this->columnsFile->acquireWrite();
        $toprint = $this->printColumns($columnDefs);
        $columnsHandle = $this->columnsFile->getHandle();
        ftruncate($columnsHandle, 0);
        fwrite($columnsHandle, $toprint);

        $this->columnsLockFile->write();

        $this->columnsFile->releaseWrite();
        $this->columnsLockFile->releaseWrite();
    }

    public function createKey($name, $type, $columns)
    {
        $key = false;
        if($type === FSQL_KEY_PRIMARY)
        {
            $key = new MemoryKey($name, $type, $columns);
            $this->keys[$name] = $key;
            addKey($name, $type, $columns);
        }
        return $key;
    }

    private function addKey($name, $type, $columns)
    {
        $colLookup = array_keys($columns);
        foreach($columns as $colIndex)
        {
            $colName = $colLookup[$colIndex];
            $oldKeyValue = $this->columns[$colName]['key'];
            if($type === FSQL_KEY_PRIMARY)
                $this->columns[$colName]['key'] = 'p';
            else if($type & FSQL_KEY_UNIQUE && $oldKeyValue !== 'p')
                $this->columns[$colName]['key'] = 'u';
            else if($oldKeyValue !== 'p' && $oldKeyValue !== 'u')
                $this->columns[$colName]['key'] = 'k';
        }
        $this->setColumns($this->columns);
        return true;
    }

    public function commit()
    {
        $writeCursor = $this->getWriteCursor();
        if(!$writeCursor->isUncommitted()) {
            return;
        }

        $this->dataLockFile->acquireWrite();
        $columnDefs = array_values($this->getColumns());
        $toprint = count($this->entries)."\r\n";
        foreach ($this->entries as $number => $entry) {
            $toprint .= $number.': ';
            foreach ($entry as $key => $value) {
                if ($value === null) {
                    $value = 'NULL';
                } elseif ($columnDefs[$key]['type'] === Types::ENUM) {
                    $index = array_search($value, $columnDefs[$key]['restraint']);
                    $value = $index !== FALSE ? $index + 1 : 0;
                } elseif (is_string($value)) {
                    $value = "'$value'";
                }
                $toprint .= $value.';';
            }
            $toprint .= "\r\n";
        }

        $this->dataFile->acquireWrite();

        $this->dataLockFile->write();

        $dataHandle = $this->dataFile->getHandle();
        ftruncate($dataHandle, 0);
        fwrite($dataHandle, $toprint);

        $this->dataFile->releaseWrite();
        $this->dataLockFile->releaseWrite();

        $this->writeCursor = null;
    }

    public function rollback()
    {
        $this->dataLockFile->reset();
    }

    public function isReadLocked()
    {
        return $this->lock === 'r';
    }

    public function readLock()
    {
        $success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
            && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
        if ($success) {
            $this->lock = 'r';

            return true;
        } else {
            $this->unlock();  // release any locks that did work if at least one failed
            return false;
        }
    }

    public function writeLock()
    {
        $success = $this->columnsLockFile->acquireWrite() && $this->columnsFile->acquireWrite()
            && $this->dataLockFile->acquireWrite() && $this->dataFile->acquireWrite();
        if ($success) {
            $this->lock = 'w';

            return true;
        } else {
            $this->unlock();  // release any locks that did work if at least one failed
            return false;
        }
    }

    public function unlock()
    {
        if ($this->lock === 'r') {
            $this->columnsLockFile->releaseRead();
            $this->columnsFile->releaseRead();
            $this->dataLockFile->releaseRead();
            $this->dataFile->releaseRead();
        } elseif ($this->lock === 'w') {
            $this->columnsLockFile->releaseWrite();
            $this->columnsFile->releaseWrite();
            $this->dataLockFile->releaseWrite();
            $this->dataFile->releaseWrite();
        }
        $this->lock = null;

        return true;
    }
}
