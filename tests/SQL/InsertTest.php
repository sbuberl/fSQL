<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class InsertTest extends BaseTest
{
    private $fsql;

    private static $columns1 = [
            'id' => [ 'type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
            'firstName' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
            'lastName' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
            'zip' => ['type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
            'gpa' => ['type' => 'f', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
            'uniform' => ['type' => 'e', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => ['S','M','L','XL']],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testFullColumnsFullValues()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, zip, gpa, uniform) VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL');");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testNoColumns()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL');");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }
}
