<?php

class fSQLTableCursor
{
    private $entries;
    private $num_rows;
    private $pos;

    public function __construct(&$entries)
    {
        $this->entries = &$entries;
        $this->first();
    }

    public function first()
    {
        $this->num_rows = count($this->entries);
        $this->pos = 0;
        return $this->pos;
    }

    public function getPosition()
    {
        return $this->pos;
    }

    public function getRow()
    {
        if($this->pos >= 0 && $this->pos < $this->num_rows)
            return $this->entries[$this->pos];
        else
            return false;
    }

    public function isDone()
    {
        return $this->pos < 0 || $this->pos >= $this->num_rows;
    }

    public function last()
    {
        $this->pos = $this->num_rows - 1;
    }

    public function previous()
    {
        $this->pos--;
    }

    public function next()
    {
        $this->pos++;
        return $this->pos;
    }

    public function seek($pos)
    {
        if($pos >=0 & $pos < count($this->entries))
            $this->pos = $pos;
    }
}

interface fSQLRelation
{
    public function name();

    public function drop();
}

abstract class fSQLTable implements fSQLRelation
{
    protected $name;
    protected $schema;
    protected $cursor = null;
    protected $columns = null;
    protected $entries = null;
    protected $identity = null;

    public function __construct(fSQLSchema $schema, $name)
    {
        $this->name = $name;
        $this->schema = $schema;
    }

    public function name()
    {
        return $this->name;
    }

    public function fullName()
    {
        return $this->schema->fullName(). '.' . $this->name;
    }

    public function schema()
    {
        return $this->schema;
    }

    public abstract function exists();

    public abstract function temporary();

    public abstract function drop();

    public abstract function truncate();

    public function getColumnNames()
    {
        return array_keys($this->getColumns());
    }

    public function getColumns()
    {
        return $this->columns;
    }

    public function setColumns($columns)
    {
        $this->columns = $columns;
    }

    public function getEntries()
    {
        return $this->entries;
    }

    public function getCursor()
    {
        if($this->cursor === null)
            $this->cursor = new fSQLTableCursor($this->entries);

        $this->cursor->first();
        return $this->cursor;
    }

    public function newCursor()
    {
        return new fSQLTableCursor($this->entries);
    }

    public function getIdentity() {
         if($this->identity === null) {
            foreach($this->getColumns() as $columnName => $column) {
                if($column['auto']) {
                    $this->identity = new fSQLIdentity($this, $columnName);
                    $this->identity->load();
                    break;
                }
            }
        }
        return $this->identity;
    }

    public function dropIdentity() {
        $this->getIdentity();
        if($this->identity !== null) {
            $columns = $this->getColumns();
            $columnName = $this->identity->getColumnName();
            $columns[$columnName]['auto'] ='0';
            $columns[$columnName]['restraint'] = array();
            $this->identity = null;
            $this->setColumns($columns);
        }
    }

    public function insertRow($data)
    {
        $this->entries[] = $data;
    }

    public function updateRow($row, $data)
    {
        foreach($data as $key=> $value)
            $this->entries[$row][$key] = $value;
    }

    public function deleteRow($row)
    {
        unset($this->entries[$row]);
    }

    public abstract function commit();

    public abstract function rollback();

    public abstract function isReadLocked();
    public abstract function readLock();
    public abstract function writeLock();
    public abstract function unlock();
}

class fSQLTempTable extends fSQLTable
{
    public function __construct(fSQLSchema $schema, $tableName, $columnDefs)
    {
        parent::__construct($schema, $tableName);
        $this->columns = $columnDefs;
        $this->entries = array();
    }

    public function exists()
    {
        return true;
    }

    public function temporary()
    {
        return true;
    }

    public function drop()
    {
    }

    public function truncate()
    {
        $this->entries = array();
    }

    /* Unecessary for temporary tables */
    public function commit() { }
    public function rollback() { }
    public function isReadLocked() { return false; }
    public function readLock() { }
    public function writeLock() { }
    public function unlock() { }
}

class fSQLCachedTable extends fSQLTable
{
    private $uncommited = false;
    public $columnsLockFile;
    public $columnsFile;
    public $dataLockFile;
    public $dataFile;
    private $lock = null;

    public function __construct(fSQLSchema $schema, $table_name)
    {
        parent::__construct($schema, $table_name);
        $path_to_schema = $this->schema->path();
        $columns_path = $path_to_schema.$table_name.'.columns';
        $data_path = $path_to_schema.$table_name.'.data';
        $this->columnsLockFile = new fSQLMicrotimeLockFile($columns_path.'.lock.cgi');
        $this->columnsFile = new fSQLFile($columns_path.'.cgi');
        $this->dataLockFile = new fSQLMicrotimeLockFile($data_path.'.lock.cgi');
        $this->dataFile = new fSQLFile($data_path.'.cgi');
    }

    public static function create(fSQLSchema $schema, $table_name, $columnDefs)
    {
        $table = new fSQLCachedTable($schema, $table_name);
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

    private function printColumns($columnDefs)
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

    public function exists()
    {
        return file_exists($this->columnsFile->getPath());
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
                        } else if($columnDefs[$m]['type'] === FSQL_TYPE_ENUM) {
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

    public function insertRow($data) {
        $this->loadEntries();
        parent::insertRow($data);
        $this->uncommited = true;
    }

    public function updateRow($row, $data) {
        $this->loadEntries();
        parent::updateRow($row, $data);
        $this->uncommited = true;
    }

    public function deleteRow($row) {
        $this->loadEntries();
        parent::deleteRow($row);
        $this->uncommited = true;
    }

    public function setColumns($columnDefs)
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
        if($success) {
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
        if($success) {
            $this->lock = 'w';
            return true;
        } else {
            $this->unlock();  // release any locks that did work if at least one failed
            return false;
        }
    }

    public function unlock()
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

class fSQLSchema
{
    private $name = null;
    private $path = null;
    private $database = null;
    private $loadedTables = array();
    private $sequencesFile;

    public function __construct(fSQLDatabase $database, $name)
    {
        $this->database = $database;
        $this->name = $name;
        $this->path = $name !== 'public' ? $database->path().$name.'/' : $database->path();
        $this->sequencesFile = new fSQLSequencesFile($this);
    }

    public function name()
    {
        return $this->name;
    }

    public function fullName()
    {
        return $this->database->name(). '.' . $this->name;
    }

    public function path()
    {
        return $this->path;
    }

    public function database()
    {
        return $this->database;
    }

    public function create()
    {
        $path = fsql_create_directory($this->path, 'schema', $this->database->environment());
        if($path !== false) {
            $this->path = $path;
            return true;
        }
        else
            return false;
    }

    public function drop()
    {
        $tables = $this->listTables();

        foreach($tables as $table) {
            $this->dropTable($table);
        }

        if($this->sequencesFile->exists()) {
            $this->sequencesFile->drop();
        }
    }

    public function createTable($table_name, $columns, $temporary = false)
    {
        if(!$temporary) {
            return fSQLCachedTable::create($this, $table_name, $columns);
        } else {
            $table = new fSQLTempTable($this, $table_name, $columns);
            $this->loadedTables[$table_name] = $table;
            return $table;
        }
    }

    public function getRelation($name)
    {
        $table = $this->getTable($name);
        if($table->exists()) {
            return $table;
        }

        $sequence = $this->getSequence($name);
        if($sequence !== false) {
            return $sequence;
        }

        return false;
    }

    public function getSequence($name)
    {
        $sequence = $this->sequencesFile->getSequence($name);
        if($sequence !== false)
            return $sequence;
        return false;
    }

    public function getTable($table_name)
    {
        if(!isset($this->loadedTables[$table_name])) {
            $table = new fSQLCachedTable($this, $table_name);
            $this->loadedTables[$table_name] = $table;
        }

        return $this->loadedTables[$table_name];
    }

    public function getSequences()
    {
        return $this->sequencesFile;
    }

    public function listTables()
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

    public function renameTable($old_table_name, $new_table_name, fSQLSchema $new_schema)
    {
        $oldTable = $this->getTable($old_table_name);
        if($oldTable->exists()) {
            if(!$oldTable->temporary()) {
                $newTable = $new_schema->createTable($new_table_name,  $oldTable->getColumns());
                copy($oldTable->dataFile->getPath(), $newTable->dataFile->getPath());
                copy($oldTable->dataLockFile->getPath(), $newTable->dataLockFile->getPath());
                $this->dropTable($old_table_name);
            } else {
                $new_schema->loadedTables[$new_table_name] = $this->loadedTables[$old_table_name];
                unset($this->loadedTables[$old_table_name]);
            }

            return true;
        } else {
            return false;
        }
    }

    public function dropTable($table_name)
    {
        $table = $this->getTable($table_name);
        if($table->exists()) {
            $table->drop();
            unset($this->loadedTables[$table_name]);
            return true;
        } else {
            return false;
        }
    }
}

class fSQLDatabase
{
    private $name = null;
    private $path = null;
    private $environment = null;
    private $schemas = array();

    public function __construct(fSQLEnvironment $environment, $name, $filePath)
    {
        $this->environment = $environment;
        $this->name = $name;
        $this->path = $filePath;
    }

    public function name()
    {
        return $this->name;
    }

    public function path()
    {
        return $this->path;
    }

    public function environment()
    {
        return $this->environment;
    }

    public function create()
    {
        $path = fsql_create_directory($this->path, 'database', $this->environment);
        if($path !== false) {
            $this->path = $path;
            return $this->defineSchema('public');
        } else {
            return false;
        }
    }

    public function drop()
    {
        foreach($this->schemas as $schema)
            $schema->drop();
        $this->schemas = array();
    }

    public function defineSchema($name)
    {
        if(!isset($this->schemas[$name])) {
            $schema = new fSQLSchema($this, $name);
            if($schema->create()) {
                $this->schemas[$name] = $schema;
            } else {
                return false;
            }
        }

        return true;
    }

    public function getSchema($name)
    {
        if(isset($this->schemas[$name])) {
            return $this->schemas[$name];
        }

        return false;
    }

    public function listSchemas()
    {
        return array_keys($this->schemas);
    }

    public function dropSchema($name)
    {
        if(isset($this->schemas[$name])) {
            $this->schemas[$name]->drop();
            unset($this->schemas[$name]);
            return true;
        }

        return false;
    }
}

abstract class fSQLSequenceBase
{
    private $lockFile;
    public $current;
    public $start;
    public $increment;
    public $min;
    public $max;
    public $cycle;
    protected $lastValue = null;

    public function __construct(fSQLMicrotimeLockFile $lockFile)
    {
        $this->lockFile = $lockFile;
    }

    public function lastValue()
    {
        return $this->lastValue;
    }

    public abstract function load();

    public abstract function save();

    private function lockAndReload()
    {
        $this->lockFile->acquireWrite();
        if($this->lockFile->wasModified()) {
            $this->load();
        }
    }

    private function saveAndUnlock()
    {
        $this->save();

        $this->lockFile->releaseWrite();
    }

    public function set($current,$start,$increment,$min,$max,$cycle)
    {
        $this->current = $current;
        $this->start = $start;
        $this->increment = $increment;
        $this->min = $min;
        $this->max = $max;
        $this->cycle = $cycle;
    }

    public function alter($updates) {
        $this->lockAndReload();

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
        }

        $this->saveAndUnlock();

        return true;
    }

    public function nextValueFor()
    {
        $this->lockAndReload();

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
        $this->lastValue = $current;
        $this->current += $this->increment;

        $this->saveAndUnlock();

        return $current;
    }

    public function restart()
    {
        $this->lockAndReload();

        $this->current = $this->start;

        $this->saveAndUnlock();
    }
}

class fSQLIdentity extends fSQLSequenceBase
{
    private $table;
    private $columnName;
    private $always;

    public function __construct(fSQLTable $table, $columnName)
    {
        parent::__construct($table->columnsLockFile);
        $this->table = $table;
        $this->columnName = $columnName;
    }

    public function getColumnName()
    {
        return $this->columnName;
    }

    public function getAlways()
    {
        $this->load();
        return $this->always;
    }

    public function load()
    {
        $columns = $this->table->getColumns();
        $identity = $columns[$this->columnName]['restraint'];
        list($current, $always, $start, $increment, $min, $max, $cycle) = $identity;
        $this->always = $always;
        $this->set($current, $start, $increment, $min, $max, $cycle);
    }

    public function save()
    {
        $columns = $this->table->getColumns();
        $columns[$this->columnName]['restraint'] = array($this->current, $this->always,
            $this->start, $this->increment, $this->min, $this->max, $this->cycle);
        $this->table->setColumns($columns);
    }

    public function alter($updates) {
        if(array_key_exists('ALWAYS', $updates)) {
            $this->always = (int) $updates['ALWAYS'];
        }

        return parent::alter($updates);
    }
}

class fSQLSequence extends fSQLSequenceBase implements fSQLRelation
{
    private $name;
    private $file;

    public function __construct($name, fSQLSequencesFile $file)
    {
        parent::__construct($file->lockFile);
        $this->name = $name;
        $this->file = $file;
    }

    public function name()
    {
        return $this->name;
    }

    public function drop()
    {
        return $this->file->dropSequence($this->name);
    }

    public function fullName()
    {
        return $this->file->schema()->name() . '.' . $this->name;
    }

    public function load()
    {
        $this->file->reload();
    }

    public function save()
    {
        $this->file->save();
    }
}

class fSQLSequencesFile
{
    private $schema;
    private $sequences;
    private $file;
    public $lockFile;

    public function __construct(fSQLSchema $schema)
    {
        $this->schema = $schema;
        $path = $schema->path().'sequences';
        $this->sequences = array();
        $this->file = new fSQLFile($path.'.cgi');
        $this->lockFile = new fSQLMicrotimeLockFile($path.'.lock.cgi');
    }

    public function create()
    {
        $this->lockFile->write();
        $this->lockFile->reset();

        $this->file->acquireWrite();
        fwrite($this->file->getHandle(), "");
        $this->file->releaseWrite();

        return true;
    }

    public function schema()
    {
        return $this->schema;
    }

    public function exists()
    {
        return $this->file->exists();
    }

    public function isEmpty()
    {
        return empty($this->sequences);
    }

    public function drop()
    {
        $this->lockFile->drop();
        $this->file->drop();
    }

    public function addSequence($name, $start, $increment, $min, $max, $cycle)
    {
        $this->lockFile->acquireWrite();
        $this->file->acquireWrite();

        $this->reload();

        $sequence = new fSQLSequence($name, $this);
        $sequence->set($start,$start,$increment,$min,$max,$cycle);
        $this->sequences[$name] = $sequence;

        $fileHandle = $this->file->getHandle();
        fseek($fileHandle, 0, SEEK_END);
        fprintf($fileHandle, "%s: %d;%d;%d;%d;%d;%d\r\n", $name, $start,
            $start,$increment,$min,$max,$cycle);

        $this->file->releaseWrite();
        $this->lockFile->releaseWrite();
    }

    public function getSequence($name)
    {
        $sequence = false;
        if($this->exists())
        {
            $this->lockFile->acquireRead();
            $this->reload();

            if(isset($this->sequences[$name])) {
                $sequence = $this->sequences[$name];
            }
            $this->lockFile->releaseRead();
        }
        return $sequence;
    }

    public function dropSequence($name)
    {
        $this->lockFile->acquireWrite();
        $this->reload();
        if(isset($this->sequences[$name])) {
            unset($this->sequences[$name]);
        }

        $this->save();
        $this->lockFile->releaseWrite();
    }

    public function reload()
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
                    $this->sequences[$name] = new fSQLSequence($name, $this);
                }
                $this->sequences[$name]->set($current,$start,$increment,$min,$max,$cycle);
            }

            $this->file->releaseRead();
        }
        $this->lockFile->releaseRead();
    }

    public function save()
    {
        $this->lockFile->acquireWrite();
        $this->file->acquireWrite();

        $this->lockFile->write();

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
