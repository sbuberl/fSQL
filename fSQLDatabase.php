<?php

function _fsql_abstract_method()
{
    trigger_error('This method is abstract. It should be overriden',E_USER_ERROR);
}

class fSQLTableCursor
{
    var $entries;
    var $num_rows;
    var $pos;

    function close() {
        unset($this->entries, $this->num_rows, $this->pos);
    }

    function first()
    {
        $this->pos = 0;
        return $this->pos;
    }

    function getPosition()
    {
        return $this->pos;
    }

    function getRow()
    {
        if($this->pos >= 0 && $this->pos < $this->num_rows)
            return $this->entries[$this->pos];
        else
            return false;
    }

    function isDone()
    {
        return $this->pos < 0 || $this->pos >= $this->num_rows;
    }

    function last()
    {
        $this->pos = $this->num_rows - 1;
    }

    function previous()
    {
        $this->pos--;
    }

    function next()
    {
        $this->pos++;
        return $this->pos;
    }

    function seek($pos)
    {
        if($pos >=0 & $pos < count($this->entries))
            $this->pos = $pos;
    }
}

class fSQLTable
{
    var $name;
    var $database;
    var $cursor = null;
    var $columns = null;
    var $entries = null;

    function fSQLTable(&$database, $name)
    {
        $this->name = $name;
        $this->database =& $database;
    }

    function close()
    {
        unset($this->name, $this->database, $this->cursor, $this->columns, $this->entries);
    }

    function name()
    {
        return $this->name;
    }

    function fullName()
    {
        return $this->database->name(). '.' . $this->name;
    }

    function &database()
    {
        return $this->database;
    }

    function exists()
    {
        _fsql_abstract_method();
    }

    function temporary()
    {
        _fsql_abstract_method();
    }

    function drop()
    {
        _fsql_abstract_method();
    }

    function truncate()
    {
        _fsql_abstract_method();
    }

    function getColumnNames()
    {
        return array_keys($this->getColumns());
    }

    function getColumns()
    {
        return $this->columns;
    }

    function setColumns($columns)
    {
        $this->columns = $columns;
    }

    function getEntries()
    {
        return $this->entries;
    }

    function &getCursor()
    {
        if($this->cursor === null)
            $this->cursor =& new fSQLTableCursor;

        $this->cursor->entries =& $this->entries;
        $this->cursor->num_rows = count($this->entries);
        $this->cursor->pos = 0;

        return $this->cursor;
    }

    function newCursor()
    {
        $cursor =& new fSQLTableCursor;
        $cursor->entries =& $this->entries;
        $cursor->num_rows = count($this->entries);
        $cursor->pos = 0;

        return $cursor;
    }

    function nextValueFor($column)
    {
        if(!isset($this->columns[$column]) || !$this->columns[$column]['auto'])
            return false;

        $this->getColumns();  // refresh columns?
        list($current, , $start, $increment, $min, $max, $canCycle) = $this->columns[$column]['restraint'];

        $cycled = false;
        if($increment > 0 && $current > $max)
        {
            $current = $min;
            $this->columns[$column]['restraint'][0] = $min;
            $cycled = true;
        }
        else if($increment < 0 && $current < $min)
        {
            $current = $max;
            $this->columns[$column]['restraint'][0] = $max;
            $cycled = true;
        }

        if($cycled && !$canCycle)
            return false;

        $this->columns[$column]['restraint'][0] += $increment;

        $this->setColumns($this->columns);

        return $current;
    }

    function restartIdentity()
    {
        $this->getColumns();
        foreach(array_keys($this->columns) as $columnName) {
            if($this->columns[$columnName]['auto']) {
                $start = $this->columns[$columnName]['restraint'][2];

                $this->columns[$columnName]['restraint'][0] = $start;

                $this->setColumns($this->columns);

                return $start;
            }
        }

        return false;
    }

    function insertRow($data)
    {
        $this->entries[] = $data;
    }

    function updateRow($row, $data)
    {
        foreach($data as $key=> $value)
            $this->entries[$row][$key] = $value;
    }

    function deleteRow($row)
    {
        unset($this->entries[$row]);
    }

    function commit()
    {
        _fsql_abstract_method();
    }

    function rollback()
    {
        _fsql_abstract_method();
    }

    function isReadLocked() { _fsql_abstract_method(); }
    function readLock() { _fsql_abstract_method(); }
    function writeLock() { _fsql_abstract_method(); }
    function unlock() { _fsql_abstract_method(); }
}

class fSQLTempTable extends fSQLTable
{
    function fSQLTempTable(&$database, $tableName, $columnDefs)
    {
        parent::fSQLTable($database, $tableName);
        $this->columns = $columnDefs;
        $this->entries = array();
    }

    function exists()
    {
        return true;
    }

    function temporary()
    {
        return true;
    }

    function drop()
    {
        $this->close();
    }

    function truncate()
    {
        $this->enries = array();
    }

    /* Unecessary for temporary tables */
    function commit() { }
    function rollback() { }
    function isReadLocked() { return false; }
    function readLock() { }
    function writeLock() { }
    function unlock() { }
}

class fSQLCachedTable extends fSQLTable
{
    var $uncommited = false;
    var $columnsLockFile;
    var $columnsFile;
    var $dataLockFile;
    var $dataFile;
    var $lock = null;

    function fSQLCachedTable(&$database, $table_name)
    {
        parent::fSQLTable($database, $table_name);
        $path_to_db = $this->database->path();
        $columns_path = $path_to_db.$table_name.'.columns';
        $data_path = $path_to_db.$table_name.'.data';
        $this->columnsLockFile = new fSQLMicrotimeLockFile($columns_path.'.lock.cgi');
        $this->columnsFile = new fSQLFile($columns_path.'.cgi');
        $this->dataLockFile = new fSQLMicrotimeLockFile($data_path.'.lock.cgi');
        $this->dataFile = new fSQLFile($data_path.'.cgi');
    }

    function close()
    {
        $this->columnsFile->close();
        $this->columnsLockFile->close();
        $this->dataFile->close();
        $this->dataLockFile->close();
        unset($this->uncommited, $this->columnsFile, $this->columnsLockFile, $this->dataLockFile,
            $this->dataFile, $this->lock);
    }

    function &create(&$database, $table_name, $columnDefs)
    {
        $table =& new fSQLCachedTable($database, $table_name);
        $table->columns = $columnDefs;

        // create the columns lock
        $table->columnsLockFile->write();
        $table->columnsLockFile->reset();

        // create the columns file
        $table->columnsFile->acquireWrite();
        $toprint = $table->_printColumns($columnDefs);
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

    function _printColumns($columnDefs)
    {
        $toprint = count($columnDefs)."\r\n";
        foreach($columnDefs as $name => $column) {
            $default = $column['default'];
            $type = $column['type'];
            $auto = $column['auto'];
            if(is_string($default) && $default !== "NULL") {
                $default = "'$default'";
            }

            $restraint = '';
            if($type === FSQL_TYPE_ENUM) {
                $restraint= "'".implode("','", $column['restraint'])."'";
            } else if($auto) {
                $restraint = implode(",", $column['restraint']);
            }

            $toprint .= $name.": ".$type.";".$restraint.";".$auto.";".$default.";".$column['key'].";".$column['null'].";\r\n";
        }
        return $toprint;
    }

    function exists()
    {
        return file_exists($this->columnsFile->getPath());
    }

    function temporary()
    {
        return false;
    }

    function drop()
    {
        $this->columnsFile->drop();
        $this->columnsLockFile->drop();
        $this->dataFile->drop();
        $this->dataLockFile->drop();
        $this->close();
    }

    function truncate()
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

    function copyTo($destination)
    {
        $destName = $destination.$this->name;
        copy($this->columnsFile->getPath(), $destName.'.columns.cgi');
        copy($this->columnsLockFile->getPath(), $destName.'.columns.lock.cgi');
        copy($this->dataFile->getPath(), $destName.'.data.cgi');
        copy($this->dataLockFile->getPath(), $destName.'.data.lock.cgi');
    }

    function copyFrom($source)
    {
        $sourceName = $source.$this->name;
        copy($sourceName.'.columns.cgi', $this->columnsFile->getPath());
        copy($sourceName.'.columns.lock.cgi', $this->columnsLockFile->getPath());
        copy($sourceName.'.data.cgi', $this->dataFile->getPath());
        copy($sourceName.'.data.lock.cgi', $this->dataLockFile->getPath());
    }

    function getColumns()
    {
        $this->columnsLockFile->acquireRead();
        if($this->columnsLockFile->wasModified())
        {
            $this->columnsLockFile->accept();

            $this->columnsFile->acquireRead();
            $columnsHandle = $this->columnsFile->getHandle();

            $line = fgets($columnsHandle);
            if(!preg_match("/^(\d+)/", $line, $matches))
            {
                $this->columnsFile->releaseRead();
                $this->columnsLockFile->releaseRead();
                return false;
            }

            $num_columns = $matches[1];

            for($i = 0; $i < $num_columns; $i++) {
                $line =    fgets($columnsHandle, 4096);
                if(preg_match("/(\S+): (dt|d|i|f|s|t|e);(.*);(0|1);(-?\d+(?:\.\d+)?|'.*'|NULL);(p|u|k|n);(0|1);/", $line, $matches)) {
                    $name = $matches[1];
                    $type = $matches[2];
                    $restraintString = $matches[3];
                    $auto = $matches[4];
                    $default = $matches[5];

                    if($type === FSQL_TYPE_INTEGER)
                        $default = (int) $default;
                    else if($type === FSQL_TYPE_FLOAT)
                        $default = (float) $default;
                    else if($default{0} == "'" && substr($default, -1) == "'") {
                        $default = substr($default, 1, -1);
                    } else if($default === "NULL") {
                        $default = null;
                    }

                    if($auto === '1' && !empty($restraintString)) {
                        list($current, $always, $start, $increment, $min, $max, $cycle) = explode(',', $restraintString);
                        $restraint = array((int) $current, (int) $always, (int) $start, (int) $increment, (int) $min, (int) $max, (int) $cycle);
                    } else if($type === FSQL_TYPE_ENUM && preg_match_all("/'(.*?(?<!\\\\))'/", $restraintString, $enumMatches) !== false) {
                        $restraint = $enumMatches[1];
                    } else {
                        $restraint = array();
                    }

                    $this->columns[$name] = array(
                        'type' => $type, 'auto' => $auto, 'default' => $default, 'key' => $matches[6], 'null' => $matches[7], 'restraint' => $restraint
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

        return $this->columns;
    }

    function getEntries()
    {
        $this->_loadEntries();
        return $this->entries;
    }

    function &getCursor()
    {
        $this->_loadEntries();

        return parent::getCursor();
    }

    function newCursor()
    {
        $this->_loadEntries();

        return parent::newCursor();
    }

    function _loadEntries()
    {
        $this->dataLockFile->acquireRead();
        if($this->dataLockFile->wasModified())
        {
            $this->dataLockFile->accept();

            $entries = null;
            $this->dataFile->acquireRead();
            $dataHandle = $this->dataFile->getHandle();

            $line = fgets($dataHandle);
            if(!preg_match("/^(\d+)/", $line, $matches))
            {
                $this->dataFile->releaseRead();
                $this->dataLockFile->releaseRead();
                return false;
            }

            $num_entries = rtrim($matches[1]);

            if($num_entries != 0)
            {
                $skip = false;
                $entries = array();

                $columnDefs = array_values($this->getColumns());
                for($i = 0;  $i < $num_entries; $i++) {
                    $line = rtrim(fgets($dataHandle, 4096));

                    if(!$skip) {
                        if(preg_match("/^(\d+):(.*)$/", $line, $matches))
                        {
                            $row = $matches[1];
                            $data = trim($matches[2]);
                        }
                        else
                            continue;
                    }
                    else {
                        $data .= $line;
                    }

                    if(!preg_match("/(-?\d+(?:\.\d+)?|'.*?(?<!\\\\)'|NULL);$/", $line)) {
                        $skip = true;
                        continue;
                    } else {
                        $skip = false;
                    }

                    preg_match_all("#((-?\d+(?:\.\d+)?)|'.*?(?<!\\\\)'|NULL);#s", $data, $matches);
                    for($m = 0; $m < count($matches[0]); $m++) {
                        if($matches[1][$m] === 'NULL') {
                            $entries[$row][$m] = null;
                        } else if(!empty($matches[2][$m])) {
                            $number = $matches[2][$m];
                            if(strpos($number, '.') !== false) {
                                $number = (float) $number;
                            } else {
                                $number = (int) $number;
                            }
                            $entries[$row][$m] = $number;
                        } else if($columnDefs['type'] === FSQL_TYPE_ENUM) {
                            $index = (int) $matches[2][$m];
                            $entries[$row][$m] = $index > 0 ? $columnDefs[$m]['restraint'][$index] : "";
                        } else {
                            $entries[$row][$m] = $matches[1][$m];
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

    function insertRow($data) {
        $this->_loadEntries();
        parent::insertRow($data);
        $this->uncommited = true;
    }

    function updateRow($row, $data) {
        $this->_loadEntries();
        parent::updateRow($row, $data);
        $this->uncommited = true;
    }

    function deleteRow($row) {
        $this->_loadEntries();
        parent::deleteRow($row);
        $this->uncommited = true;
    }

    function setColumns($columnDefs)
    {
        $this->columnsLockFile->acquireWrite();

        $this->columns = $columnDefs;

        $this->columnsFile->acquireWrite();
        $toprint = $this->_printColumns($columnDefs);
        $columnsHandle = $this->columnsFile->getHandle();
        ftruncate($columnsHandle, 0);
        fwrite($columnsHandle, $toprint);

        $this->columnsFile->releaseWrite();
        $this->columnsLockFile->releaseWrite();
    }

    function commit()
    {
        if($this->uncommited === false)
            return;

        $this->dataLockFile->acquireWrite();
        $columnDefs = array_values($this->getColumns());
        $toprint = count($this->entries)."\r\n";
        foreach($this->entries as $number => $entry) {
            $toprint .= $number.': ';
            foreach($entry as $key => $value) {
                if($value === NULL) {
                    $value = 'NULL';
                } else if($columnDefs[$key]['type'] === FSQL_TYPE_ENUM) {
                    $value = (int) array_search($value, $columnDefs[$key]['restraint']);;
                } else if(is_string($value)) {
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

    function rollback()
    {
        $this->dataLockFile->reset();
        $this->uncommited = false;
    }

    function isReadLocked()
    {
        return $this->lock === 'r';
    }

    function readLock()
    {
        $success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
            && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
        if($success) {
            $this->lock = 'r';
            return true;
        } else {
            $this->unlock();  // release any locks that did work if at least one failed
            return false;
        }
    }

    function writeLock()
    {
        $success = $this->columnsLockFile->acquireRead() && $this->columnsFile->acquireRead()
            && $this->dataLockFile->acquireRead() && $this->dataFile->acquireRead();
        if($success) {
            $this->lock = 'w';
            return true;
        } else {
            $this->unlock();  // release any locks that did work if at least one failed
            return false;
        }
    }

    function unlock()
    {
        if($this->lock === 'r')
        {
            $this->columnsLockFile->releaseRead();
            $this->columnsFile->releaseRead();
            $this->dataLockFile->releaseRead();
            $this->dataFile->releaseRead();
        }
        else if($this->lock === 'w')
        {
            $this->columnsLockFile->releaseWrite();
            $this->columnsFile->releaseWrite();
            $this->dataLockFile->releaseWrite();
            $this->dataFile->releaseWrite();
        }
        $this->lock = null;
        return true;
    }
}

class fSQLDatabase
{
    var $name = null;
    var $path = null;
    var $loadedTables = array();

    function fSQLDatabase($name, $filePath)
    {
        $this->name = $name;
        $this->path = $filePath;
    }

    function close()
    {
        foreach(array_keys($this->loadedTables) as $table_name) {
            $table =& $this->loadedTables[$table_name];
            $table->close();
            $table = null;
            unset($table);
        }

        unset($this->name, $this->path, $this->loadedTables);
    }

    function name()
    {
        return $this->name;
    }

    function path()
    {
        return $this->path;
    }

    function &createTable($table_name, $columns, $temporary = false)
    {
        $table = false;

        if(!$temporary) {
            $table =& fSQLCachedTable::create($this, $table_name, $columns);
        } else {
            $table =& new fSQLTempTable($this, $table_name, $columns);
            $this->loadedTables[$table_name] =& $table;
        }

        return $table;
    }

    function &getTable($table_name)
    {
        if(!isset($this->loadedTables[$table_name])) {
            $table =& new fSQLCachedTable($this, $table_name);
            $this->loadedTables[$table_name] =& $table;
            unset($table);
        }

        return $this->loadedTables[$table_name];
    }

    function listTables()
    {
        $tables = array();
        if(file_exists($this->path) && is_dir($this->path)) {
            $dir = opendir($this->path);
            while (false !== ($file = readdir($dir))) {
                if ($file != '.' && $file != '..' && !is_dir($file)) {
                    if(substr($file, -12) == '.columns.cgi') {
                        $tables[] = substr($file, 0, -12);
                    }
                }
            }
            closedir($dir);
        }

        return $tables;
    }

    function renameTable($old_table_name, $new_table_name, &$new_db)
    {
        $oldTable =& $this->getTable($old_table_name);
        if($oldTable->exists()) {
            if(!$oldTable->temporary()) {
                $newTable = $new_db->createTable($new_table_name,  $oldTable->getColumns());
                copy($oldTable->dataFile->getPath(), $newTable->dataFile->getPath());
                copy($oldTable->dataLockFile->getPath(), $newTable->dataLockFile->getPath());
                $this->dropTable($old_table_name);
            } else {
                $new_db->loadedTables[$new_table_name] =& $this->loadedTables[$old_table_name];
                unset($this->loadedTables[$old_table_name]);
            }

            return true;
        } else {
            return false;
        }
    }

    function dropTable($table_name)
    {
        $table =& $this->getTable($table_name);
        if($table->exists()) {
            $table->drop();

            $table = null;
            unset($this->loadedTables[$table_name]);
            unset($table);

            return true;
        } else {
            return false;
        }
    }
}

?>
