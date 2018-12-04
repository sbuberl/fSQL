<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class AlterTableTest extends BaseTest
{
    private $fsql;

    private static $columnsWithKey = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => 'blah', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'gpa' => ['type' => 'f', 'auto' => 0, 'default' => 11000000.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    private static $columnsWithoutKey = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => 'blah', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'gpa' => ['type' => 'f', 'auto' => 0, 'default' => 11000000.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testAlterTableWrongDB()
    {
        $dbName = 'wrongDB';
        $result = $this->fsql->query("ALTER TABLE $dbName.public.students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Database $dbName not found", trim($this->fsql->error()));
    }

    public function testAlterTableWrongSchema()
    {
        $schemaName = 'wrongSchema';
        $result = $this->fsql->query("ALTER TABLE $schemaName.students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Schema db1.$schemaName does not exist", trim($this->fsql->error()));
    }

    public function testAlterTableNoExist()
    {
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Table db1.public.students does not exist", trim($this->fsql->error()));
    }

    public function testAlterTableIfExistsNoExist()
    {
        $result = $this->fsql->query("ALTER TABLE IF EXISTS students ADD PRIMARY KEY(id)");
        $this->assertTrue($result);
    }

    public function testAlterTableAddPrimaryKeyExists()
    {
        CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Primary key already exists", trim($this->fsql->error()));
    }

    public function testAlterTableAddPrimaryKeyBadColumn()
    {
        CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(blah)");
        $this->assertFalse($result);
        $this->assertEquals("Column named 'blah' does not exist in table 'students'", trim($this->fsql->error()));
    }

    public function testAlterTableAddPrimaryKey()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(id)");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['id']['key'], 'p');
    }
}
