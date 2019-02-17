K<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class LocksTest extends BaseTest
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

    public function testLockTableNoExist()
    {
        $result = $this->fsql->query("LOCK TABLES myTable READ LOCAL;");
        $this->assertFalse($result);
        $this->assertEquals("Table db1.public.myTable does not exist", trim($this->fsql->error()));
    }

    public function testLockTableRead()
    {
        CachedTable::create($this->fsql->current_schema(), 'myTable', self::$columns);
        $result = $this->fsql->query("LOCK TABLES myTable READ LOCAL;");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('myTable');
        $this->assertTrue($table->isReadLocked());

        $result = $this->fsql->query("UNLOCK TABLES;");
        $this->assertTrue($result);
        $this->assertFalse($table->isReadLocked());
    }

    public function testLockTableWrite()
    {
        CachedTable::create($this->fsql->current_schema(), 'myTable', self::$columns);
        $result = $this->fsql->query("LOCK TABLES myTable LOW PRIORITY WRITE;");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('myTable');
        $this->assertTrue($table->isWriteLocked());

        $result = $this->fsql->query("UNLOCK TABLES;");
        $this->assertTrue($result);
        $this->assertFalse($table->isWriteLocked());
    }

    public function testLockTableMultiple()
    {
        CachedTable::create($this->fsql->current_schema(), 'myTable', self::$columns);
        CachedTable::create($this->fsql->current_schema(), 'myTable2', self::$columns);
        $result = $this->fsql->query("LOCK TABLES myTable READ, myTable2 WRITE;");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('myTable');
        $this->assertTrue($table->isReadLocked());

        $table2 = $this->fsql->current_schema()->getTable('myTable2');
        $this->assertTrue($table2->isWriteLocked());

        $result = $this->fsql->query("UNLOCK TABLES;");
        $this->assertTrue($result);
        $this->assertFalse($table->isReadLocked());
        $this->assertFalse($table2->isWriteLocked());
    }
}
