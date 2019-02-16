<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class RenameTableTest extends BaseTest
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
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testRenameSourceDoesNotExist()
    {
        $result = $this->fsql->query("RENAME TABLE myTable TO students");
        $this->assertFalse($result);
        $this->assertEquals("Table db1.public.myTable does not exist", trim($this->fsql->error()));
    }

    public function testRenameDestDoesAlreadyExists() {
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "students", self::$columns1);
        CachedTable::create($schema, "table2", self::$columns2);

        $result = $this->fsql->query("RENAME TABLE table2 TO students");
        $this->assertFalse($result);
        $this->assertEquals("Destination table db1.public.students already exists", trim($this->fsql->error()));
    }

    public function testRenameSuccess() {
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "table2", self::$columns2);

        $result = $this->fsql->query("RENAME TABLE table2 TO students");
        $this->assertTrue($result);

        $table2 = $schema->getTable('table2');
        $this->assertTrue($table2 !== false);
        $this->assertFalse($table2->exists());

        $students = $schema->getTable('students');
        $this->assertTrue($students !== false);
        $this->assertTrue($students->exists());
    }

    public function testRenameMultiple() {
        $schema = $this->fsql->current_schema();
        CachedTable::create($schema, "table", self::$columns2);
        CachedTable::create($schema, "students", self::$columns2);

        $result = $this->fsql->query("RENAME TABLE table TO table2, students TO students2");
        $this->assertTrue($result);

        $table = $schema->getTable('table');
        $this->assertTrue($table !== false);
        $this->assertFalse($table->exists());

        $table2 = $schema->getTable('table2');
        $this->assertTrue($table2 !== false);
        $this->assertTrue($table2->exists());

        $students = $schema->getTable('students');
        $this->assertTrue($students !== false);
        $this->assertFalse($students->exists());

        $students2 = $schema->getTable('students2');
        $this->assertTrue($students2 !== false);
        $this->assertTrue($students2->exists());
    }
}
