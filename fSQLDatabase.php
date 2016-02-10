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
    var $identity = null;

    function fSQLTable(&$database, $name)
    {
        $this->name = $name;
        $this->database =& $database;
    }

    function close()
    {
        unset($this->name, $this->database, $this->cursor, $this->columns, $this->entries, $this->identity);
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

    function &getIdentity() {
         if($this->identity === null) {
            foreach($this->getColumns() as $columnName => $column) {
                if($column['auto']) {
                    $this->identity =& new fSQLIdentity($this, $columnName);
                    $this->identity->load();
                    break;
                }
            }
        }
        return $this->identity;
    }

    function dropIdentity() {
        $this->getIdentity();
        if($this->identity !== null) {
            $columns = $this->getColumns();
            $columnName = $this->identity->getColumnName();
            $columns[$columnName]['auto'] ='0';
            $columns[$columnName]['restraint'] = array();
            $this->identity->close();
            $this->identity = null;
            $this->setColumns($columns);
        }
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
        $this->entries = array();
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
        parent::close();
        $this->columnsFile->close();
        $this->columnsLockFile->close();
        $this->dataFile->close();
        $this->dataLockFile->close();
        unset($this->uncommited, $this->columnsFile, $this->columnsLockFile, $this->dataLockFile,
            $this->dataFile, $this->lock);
    }

    static function &create(&$database, $table_name, $columnDefs)
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
                    $value = (int) array_search($value, $columnDefs[$key]['restraint']);
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
    var $sequencesFile;

    function fSQLDatabase($name, $filePath)
    {
        $this->name = $name;
        $this->path = $filePath;
        $this->sequencesFile =& new fSQLSequencesFile($this);
    }

    function close()
    {
        $this->sequencesFile = null;
        foreach(array_keys($this->loadedTables) as $table_name) {
            $table =& $this->loadedTables[$table_name];
            $table->close();
            $table = null;
            unset($table);
        }

        unset($this->name, $this->path, $this->loadedTables, $this->sequencesFile);
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

    function &getSequences()
    {
        return $this->sequencesFile;
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

class fSQLSequenceBase
{
    var $lockFile;
    var $current;
    var $start;
    var $increment;
    var $min;
    var $max;
    var $cycle;

    function fSQLSequenceBase(&$lockFile)
    {
        $this->lockFile =& $lockFile;
    }

    function close()
    {
        unset($this->lockFile, $this->current, $this->start, $this->increment, $this->min,
            $this->max, $this->cycle);
    }

    function load()
    {
        return false;
    }

    function save()
    {
        return false;
    }

    function _lockAndReload()
    {
        $this->lockFile->acquireWrite();
        if($this->lockFile->wasModified()) {
            $this->load();
        }
    }

    function _saveAndUnlock()
    {
        $this->save();

        $this->lockFile->releaseWrite();
    }

    function set($current, $start,$increment,$min,$max,$cycle)
    {
        $this->current = $current;
        $this->start = $start;
        $this->increment = $increment;
        $this->min = $min;
        $this->max = $max;
        $this->cycle = $cycle;
    }

    function alter($updates) {
        $this->_lockAndReload();

        if(array_key_exists('INCREMENT', $updates)) {
            $this->increment = (int) $updates['INCREMENT'];
            if($this->increment === 0) {
                $this->lockFile->releaseWrite();
                return 'Increment of zero in sequence/identity defintion is not allowed';
            }
        }

        $intMax = defined('PHP_INT_MAX') ? PHP_INT_MAX : intval('420000000000000000000');
        $intMin = defined('PHP_INT_MIN') ? PHP_INT_MIN : ~$intMax;

        $climbing = $this->increment > 0;
        if(array_key_exists('MINVALUE', $updates)) {
            $this->min = isset($updates['MINVALUE']) ? (int) $updates['MINVALUE'] : ($climbing ? 1 : $intMin);
        }
        if(array_key_exists('MAXVALUE', $updates)) {
            $this->max = isset($updates['MAXVALUE']) ? (int) $updates['MAXVALUE'] : ($climbing ? $intMax : -1);
        }
        if(array_key_exists('CYCLE', $updates)) {
            $this->cycle = isset($updates['CYCLE']) ? (int) $updates['CYCLE'] : 0;
        }

        if($this->min > $this->max) {
            $this->lockFile->releaseWrite();
            return 'Sequence/identity minimum is greater than maximum';
        }

        if(isset($updates['RESTART'])) {
            $restart = $updates['RESTART'];
            $this->current = $restart !== 'start' ? (int) $restart : $this->start;
            if($this->current < $this->min || $this->current > $this->max) {
                $this->lockFile->releaseWrite();
                return 'Sequence/identity restart value not between min and max';
            }
        } else if($climbing) {
            $this->current = $this->min;
        } else {
            $this->current = $this->max;
        }

        $this->_saveAndUnlock();

        return true;
    }

    function nextValueFor()
    {
        $this->_lockAndReload();

        $cycled = false;
        if($this->increment > 0 && $this->current > $this->max) {
            $this->current = $this->min;
            $cycled = true;
        } else if($this->increment < 0 && $this->current < $this->min) {
            $this->current = $this->max;
            $cycled = true;
        }

        if($cycled && !$this->cycle) {
            $this->lockFile->releaseWrite();
            return false;
        }

        $current = $this->current;
        $this->current += $this->increment;

        $this->_saveAndUnlock();

        return $current;
    }

    function restart()
    {
        $this->_lockAndReload();

        $this->current = $this->start;

        $this->_saveAndUnlock();
    }
}

class fSQLIdentity extends fSQLSequenceBase
{
    var $table;
    var $columnName;
    var $always;

    function fSQLIdentity(&$table, $columnName)
    {
        parent::fSQLSequenceBase($table->columnsLockFile);
        $this->table =& $table;
        $this->columnName = $columnName;
    }

    function close()
    {
        parent::close();
        unset($this->table, $this->columnName, $this->always);
    }

    function getColumnName()
    {
        return $this->columnName;
    }

    function load()
    {
        $columns = $this->table->getColumns();
        $identity = $columns[$this->columnName]['restraint'];
        list($current, $always, $start, $increment, $min, $max, $cycle) = $identity;
        $this->always = $always;
        $this->set($current, $start, $increment, $min, $max, $cycle);
    }

    function save()
    {
        $columns = $this->table->getColumns();
        $columns[$this->columnName]['restraint'] = array($this->current, $this->always,
            $this->start, $this->increment, $this->min, $this->max, $this->cycle);
        $this->table->setColumns($columns);
    }

    function alter($updates) {
        if(array_key_exists('ALWAYS', $updates)) {
            $this->always = (int) $updates['ALWAYS'];
        }

        return parent::alter($updates);
    }
}

class fSQLSequence extends fSQLSequenceBase
{
    var $name;
    var $file;

    function fSQLSequence($name, &$file)
    {
        parent::fSQLSequenceBase($file->lockFile);
        $this->name = $name;
        $this->file =& $file;
    }

    function close()
    {
        parent::close();
        unset($this->name, $this->file);
    }

    function name()
    {
        return $this->name;
    }

    function load()
    {
        $this->file->reload();
    }

    function save()
    {
        $this->file->save();
    }
}

class fSQLSequencesFile
{
    var $database;
    var $sequences;
    var $file;
    var $lockFile;

    function fSQLSequencesFile(&$database)
    {
        $this->database =& $database;
        $path = $database->path().'sequences';
        $this->sequences = array();
        $this->file = new fSQLFile($path.'.cgi');
        $this->lockFile = new fSQLMicrotimeLockFile($path.'.lock.cgi');
    }

    function close()
    {
        $this->file->close();
        $this->lockFile->close();
        unset($this->database, $this->sequences, $this->file, $this->lockFile);
    }

    function create()
    {
        $this->lockFile->write();
        $this->lockFile->reset();

        $this->file->acquireWrite();
        fwrite($this->file->getHandle(), "");
        $this->file->releaseWrite();

        return true;
    }

    function exists()
    {
        return $this->file->exists();
    }

    function addSequence($name, $start, $increment, $min, $max, $cycle)
    {
        $this->lockFile->acquireWrite();
        $this->file->acquireWrite();

        $this->reload();

        $sequence =& new fSQLSequence($name, $this);
        $sequence->set($start,$start,$increment,$min,$max,$cycle);
        $this->sequences[$name] =& $sequence;

        $fileHandle = $this->file->getHandle();
        fseek($fileHandle, 0, SEEK_END);
        fprintf($fileHandle, "%s: %d;%d;%d;%d;%d;%d\r\n", $name, $start,
            $start,$increment,$min,$max,$cycle);

        $this->file->releaseWrite();
        $this->lockFile->releaseWrite();
    }

    function &getSequence($name)
    {
        $this->lockFile->acquireRead();
        $this->reload();
        $sequence = false;
        if(isset($this->sequences[$name])) {
            $sequence =& $this->sequences[$name];
        }
        $this->lockFile->releaseRead();
        return $sequence;
    }

    function dropSequence($name)
    {
        $this->lockFile->acquireWrite();
        $this->reload();
        if(isset($this->sequences[$name])) {
            $this->sequences[$name]->close();
            $this->sequences[$name] = null;
            unset($this->sequences[$name]);
        }

        $this->save();
        $this->lockFile->releaseWrite();
    }

    function reload()
    {
        $this->lockFile->acquireRead();
        if($this->lockFile->wasModified()) {
            $this->lockFile->accept();

            $this->file->acquireRead();
            $fileHandle = $this->file->getHandle();
            while(!feof($fileHandle)) {
                fscanf($fileHandle, "%[^:]: %d;%d;%d;%d;%d;%d\r\n", $name, $current,
                    $start,$increment,$min,$max,$cycle);
                if(!isset($this->sequences[$name])) {
                    $this->sequences[$name] =& new fSQLSequence($name, $this);
                }
                $this->sequences[$name]->set($current,$start,$increment,$min,$max,$cycle);
            }

            $this->file->acquireRead();
        }
        $this->lockFile->releaseRead();
    }

    function save()
    {
        $this->lockFile->acquireWrite();
        $this->file->acquireWrite();

        $fileHandle = $this->file->getHandle();
        ftruncate($fileHandle, 0);
        foreach($this->sequences as $name => $sequence) {
            fprintf($fileHandle, "%s: %d;%d;%d;%d;%d;%d\r\n", $name, $sequence->current,
                $sequence->start,$sequence->increment,$sequence->min,$sequence->max,$sequence->cycle);
        }

        $this->file->releaseWrite();
        $this->lockFile->releaseWrite();
    }
}

?>
