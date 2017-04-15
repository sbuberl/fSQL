<?php

namespace FSQL\Database;

use FSQL\File;
use FSQL\LockableFile;
use FSQL\MicrotimeLockFile;
use FSQL\Types;

class CachedTable extends Table
{
    private $uncommited = false;
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

            $toprint .= $name.': '.$type.';'.$restraint.';'.$auto.';'.$default.';'.$column['key'].';'.$column['null'].";\r\n";
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

    public function copyTo($destination)
    {
        $destName = $destination.$this->name;
        copy($this->columnsFile->getPath(), $destName.'.columns.cgi');
        copy($this->columnsLockFile->getPath(), $destName.'.columns.lock.cgi');
        copy($this->dataFile->getPath(), $destName.'.data.cgi');
        copy($this->dataLockFile->getPath(), $destName.'.data.lock.cgi');
    }

    public function copyFrom($source)
    {
        $sourceName = $source.$this->name;
        copy($sourceName.'.columns.cgi', $this->columnsFile->getPath());
        copy($sourceName.'.columns.lock.cgi', $this->columnsLockFile->getPath());
        copy($sourceName.'.data.cgi', $this->dataFile->getPath());
        copy($sourceName.'.data.lock.cgi', $this->dataLockFile->getPath());
    }

    public function getColumns()
    {
        $this->columnsLockFile->acquireRead();
        if ($this->columnsLockFile->wasModified()) {
            $this->columnsLockFile->accept();

            $this->columnsFile->acquireRead();
            $columnsHandle = $this->columnsFile->getHandle();

            $line = fgets($columnsHandle);
            if (!preg_match("/^(\d+)/", $line, $matches)) {
                $this->columnsFile->releaseRead();
                $this->columnsLockFile->releaseRead();

                return false;
            }

            $num_columns = $matches[1];

            for ($i = 0; $i < $num_columns; ++$i) {
                $line = fgets($columnsHandle, 4096);
                if (preg_match("/(\S+): (dt|d|i|f|s|t|e);(.*);(0|1);(-?\d+(?:\.\d+)?|'.*'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
                    $name = $matches[1];
                    $type = $matches[2];
                    $restraintString = $matches[3];
                    $auto = (int) $matches[4];
                    $default = $matches[5];
                    $null = (int) $matches[7];

                    if ($type === Types::INTEGER) {
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
                        $restraint = array((int) $current, (int) $always, (int) $start, (int) $increment, (int) $min, (int) $max, (int) $cycle);
                    } elseif ($type === Types::ENUM && preg_match_all("/'(.*?(?<!\\\\))'/", $restraintString, $enumMatches) !== false) {
                        $restraint = $enumMatches[1];
                    } else {
                        $restraint = array();
                    }

                    $this->columns[$name] = array(
                        'type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $matches[6], 'null' => $null, 'restraint' => $restraint,
                    );
                } else {
                    $this->columnsFile->releaseRead();
                    $this->columnsLockFile->releaseRead();

                    return false;
                }
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

    private function loadEntries()
    {
        $this->dataLockFile->acquireRead();
        if ($this->dataLockFile->wasModified()) {
            $this->dataLockFile->accept();

            $entries = array();
            $this->dataFile->acquireRead();
            $dataHandle = $this->dataFile->getHandle();

            $line = fgets($dataHandle);
            if (!preg_match("/^(\d+)/", $line, $matches)) {
                $this->dataFile->releaseRead();
                $this->dataLockFile->releaseRead();

                return false;
            }

            $num_entries = rtrim($matches[1]);

            if ($num_entries != 0) {
                $skip = false;

                $columnDefs = array_values($this->getColumns());
                for ($i = 0; $i < $num_entries; ++$i) {
                    $line = rtrim(fgets($dataHandle, 4096));

                    if (!$skip) {
                        if (preg_match("/^(\d+):(.*)$/", $line, $matches)) {
                            $row = $matches[1];
                            $data = trim($matches[2]);
                        } else {
                            continue;
                        }
                    } else {
                        $data .= $line;
                    }

                    if (!preg_match("/(-?\d+(?:\.\d+)?|'.*?(?<!\\\\)'|NULL);$/", $line)) {
                        $skip = true;
                        continue;
                    } else {
                        $skip = false;
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
                            $entries[$row][$m] = $index > 0 ? $columnDefs[$m]['restraint'][$index] : '';
                        } else {
                            $entries[$row][$m] = $matches[3][$m];
                        }
                    }
                }
            }

            $this->entries = $entries;

            $this->dataFile->releaseRead();
        }

        $this->dataLockFile->releaseRead();

        return true;
    }

    public function insertRow(array $data)
    {
        $this->loadEntries();
        parent::insertRow($data);
        $this->uncommited = true;
    }

    public function updateRow($row, array $data)
    {
        $this->loadEntries();
        parent::updateRow($row, $data);
        $this->uncommited = true;
    }

    public function deleteRow($row)
    {
        $this->loadEntries();
        parent::deleteRow($row);
        $this->uncommited = true;
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

        $this->columnsFile->releaseWrite();
        $this->columnsLockFile->releaseWrite();
    }

    public function commit()
    {
        if ($this->uncommited === false) {
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
                    $value = (int) array_search($value, $columnDefs[$key]['restraint']);
                } elseif (is_string($value)) {
                    $value = "'$value'";
                }
                $toprint .= $value.';';
            }
            $toprint .= "\r\n";
        }

        $this->dataFile->acquireWrite();

        $dataHandle = $this->dataFile->getHandle();
        ftruncate($dataHandle, 0);
        fwrite($dataHandle, $toprint);

        $this->dataFile->releaseWrite();
        $this->dataLockFile->releaseWrite();

        $this->uncommited = false;
    }

    public function rollback()
    {
        $this->dataLockFile->reset();
        $this->uncommited = false;
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
