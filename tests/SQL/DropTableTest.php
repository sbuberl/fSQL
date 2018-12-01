<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class DropTableTest extends BaseTest
{
    private $fsql;

    private static $columns = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
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

    public function testDropTableDbNoExist()
    {
        $result = $this->fsql->query("DROP TABLE wrongDB.db1.table;");
        $this->assertFalse($result);
        $this->assertEquals("Database wrongDB not found", trim($this->fsql->error()));
    }

    public function testDropTableSchemaNoExist()
    {
        $result = $this->fsql->query("DROP TABLE schema2.table;");
        $this->assertFalse($result);
        $this->assertEquals("Schema db1.schema2 does not exist", trim($this->fsql->error()));
    }

    public function testDropTableNoExist()
    {
        $result = $this->fsql->query("DROP TABLE blah;");
        $this->assertFalse($result);
        $this->assertEquals("Table db1.public.blah does not exist", trim($this->fsql->error()));
    }

    public function testDropTableIfExistsNoExist()
    {
        $result = $this->fsql->query("DROP TABLE IF EXISTS blah;");
        $this->assertTrue($result);
    }

    public function testDropTable()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns);
        $result = $this->fsql->query("DROP TABLE students;");
        $this->assertTrue($result);
    }

    public function testDropTableMultiple()
    {
        $table1 = CachedTable::create($this->fsql->current_schema(), 'students1', self::$columns);
        $table2 = CachedTable::create($this->fsql->current_schema(), 'students2', self::$columns);
        $table2 = CachedTable::create($this->fsql->current_schema(), 'students3', self::$columns);
        $result = $this->fsql->query("DROP TABLE students1, students2, students3;");
        $this->assertTrue($result);
    }
}