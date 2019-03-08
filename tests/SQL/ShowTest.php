<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class ShowTest extends BaseTest
{
    private $fsql;

    private static $columns1 = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => 'blah', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'gpa' => ['type' => 'f', 'auto' => 0, 'default' => 11000000.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    private static $columns2 = [
        'id' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'person' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'item' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'quantity' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'orderDate' => ['type' => 'd', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'total' => ['type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
    }

    public function testShowDatabasesNone()
    {
        $result = $this->fsql->query('SHOW DATABASES');
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals(null, $result);
    }

    public function testShowDatabases()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->define_db('db2', './');

        $result = $this->fsql->query('SHOW DATABASES');
        $this->assertTrue($result !== false);

        $db1 = $this->fsql->fetch_assoc($result);
        $db2 = $this->fsql->fetch_assoc($result);
        $this->assertEquals(["Name" => 'db1'], $db1);
        $this->assertEquals(["Name" => 'db2'], $db2);
    }

    public function testShowTablesNone()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');

        $result = $this->fsql->query('SHOW TABLES');
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals(null, $result);
    }

    public function testShowTables()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);
        CachedTable::create($schema, "table2", self::$columns2);

        $result = $this->fsql->query('SHOW TABLES');
        $this->assertTrue($result !== false);

        $table1 = $this->fsql->fetch_assoc($result);
        $table2 = $this->fsql->fetch_assoc($result);
        $this->assertEquals(["Name" => 'students'], $table1);
        $this->assertEquals(["Name" => 'table2'], $table2);
    }

    public function testShowTablesFull()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);
        CachedTable::create($schema, "table2", self::$columns2);

        $result = $this->fsql->query('SHOW FULL TABLES FROM db1.public');
        $this->assertTrue($result !== false);

        $table1 = $this->fsql->fetch_assoc($result);
        $table2 = $this->fsql->fetch_assoc($result);
        $this->assertEquals(["Name" => 'students', 'Table_type' => 'BASE TABLE'], $table1);
        $this->assertEquals(["Name" => 'table2', 'Table_type' => 'BASE TABLE'], $table2);
    }

    public function testShowColumns()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);

        $result = $this->fsql->query('SHOW COLUMNS IN db1.public.students');
        $this->assertTrue($result !== false);
        $expected = [
            ['Field' => 'id', 'Type' => 'INTEGER', 'Null' => 'NO', 'Default' => 0, 'Key'=>'PRI', 'Extra' => 'auto_increment'],
            ['Field' => 'firstName', 'Type' => 'TEXT', 'Null' => 'YES', 'Default' => null, 'Key' => '', 'Extra' => ''],
            ['Field' => 'lastName', 'Type' => 'TEXT', 'Null' => 'NO', 'Default' => 'blah', 'Key' => '', 'Extra' => ''],
            ['Field' => 'zip', 'Type' => 'INTEGER', 'Null' => 'NO', 'Default' => 12345, 'Key' => '', 'Extra' => ''],
            ['Field' => 'gpa', 'Type' => 'DOUBLE', 'Null' => 'NO', 'Default' => 11000000.0, 'Key' => '', 'Extra' => ''],
        ];
        $result = $this->fsql->fetch_all($result);
        $this->assertEquals($expected, $result);
    }

    public function testShowColumnFull()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);

        $result = $this->fsql->query('SHOW FULL COLUMNS IN db1.public.students');
        $this->assertTrue($result !== false);
        $expected = [
            ['Field' => 'id', 'Type' => 'INTEGER', 'Collation' => null, 'Null' => 'NO', 'Default' => 0, 'Key'=>'PRI', 'Extra' => 'auto_increment',
                'Privileges' => 'select,insert,update,references', 'Comment' => ''],
            ['Field' => 'firstName', 'Type' => 'TEXT', 'Collation' => null, 'Null' => 'YES', 'Default' => null, 'Key' => '', 'Extra' => '',
                'Privileges' => 'select,insert,update,references', 'Comment' => ''],
            ['Field' => 'lastName', 'Type' => 'TEXT', 'Collation' => null, 'Null' => 'NO', 'Default' => 'blah', 'Key' => '', 'Extra' => '',
                'Privileges' => 'select,insert,update,references', 'Comment' => ''],
            ['Field' => 'zip', 'Type' => 'INTEGER', 'Collation' => null, 'Null' => 'NO', 'Default' => 12345, 'Key' => '', 'Extra' => '',
                'Privileges' => 'select,insert,update,references', 'Comment' => ''],
            ['Field' => 'gpa', 'Type' => 'DOUBLE', 'Collation' => null, 'Null' => 'NO', 'Default' => 11000000.0, 'Key' => '', 'Extra' => '',
                'Privileges' => 'select,insert,update,references', 'Comment' => ''],
        ];
        $result = $this->fsql->fetch_all($result);
        $this->assertEquals($expected, $result);
    }

    // DESC(RIBE)? is an alias for SHOW COLUMNS so do it here
    public function testDescribe()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);

        $result = $this->fsql->query('DESCRIBE db1.public.students');
        $this->assertTrue($result !== false);
        $expected = [
            ['Field' => 'id', 'Type' => 'INTEGER', 'Null' => 'NO', 'Default' => 0, 'Key'=>'PRI', 'Extra' => 'auto_increment'],
            ['Field' => 'firstName', 'Type' => 'TEXT', 'Null' => 'YES', 'Default' => null, 'Key' => '', 'Extra' => ''],
            ['Field' => 'lastName', 'Type' => 'TEXT', 'Null' => 'NO', 'Default' => 'blah', 'Key' => '', 'Extra' => ''],
            ['Field' => 'zip', 'Type' => 'INTEGER', 'Null' => 'NO', 'Default' => 12345, 'Key' => '', 'Extra' => ''],
            ['Field' => 'gpa', 'Type' => 'DOUBLE', 'Null' => 'NO', 'Default' => 11000000.0, 'Key' => '', 'Extra' => ''],
        ];
        $result = $this->fsql->fetch_all($result);
        $this->assertEquals($expected, $result);
    }

    public function testDesc()
    {
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);

        $result = $this->fsql->query('DESC db1.public.students');
        $this->assertTrue($result !== false);
        $expected = [
            ['Field' => 'id', 'Type' => 'INTEGER', 'Null' => 'NO', 'Default' => 0, 'Key'=>'PRI', 'Extra' => 'auto_increment'],
            ['Field' => 'firstName', 'Type' => 'TEXT', 'Null' => 'YES', 'Default' => null, 'Key' => '', 'Extra' => ''],
            ['Field' => 'lastName', 'Type' => 'TEXT', 'Null' => 'NO', 'Default' => 'blah', 'Key' => '', 'Extra' => ''],
            ['Field' => 'zip', 'Type' => 'INTEGER', 'Null' => 'NO', 'Default' => 12345, 'Key' => '', 'Extra' => ''],
            ['Field' => 'gpa', 'Type' => 'DOUBLE', 'Null' => 'NO', 'Default' => 11000000.0, 'Key' => '', 'Extra' => ''],
        ];
        $result = $this->fsql->fetch_all($result);
        $this->assertEquals($expected, $result);
    }
}
