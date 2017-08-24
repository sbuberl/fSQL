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

    private static $columns2 = [
        'id' => [ 'type' => 'i', 'auto' => 0, 'default' => 12, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => 'Jane', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => 'Doe', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'gpa' => ['type' => 'f', 'auto' => 0, 'default' => 4.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'uniform' => ['type' => 'e', 'auto' => 0, 'default' => 3, 'key' => 'n', 'null' => 0, 'restraint' => ['S','M','L','XL']],
    ];

    private static $columns3 = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => 'blah', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'gpa' => ['type' => 'f', 'auto' => 0, 'default' => 11000000.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    private static $columns4 = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 1, 1, 1, 1, 10000, 0]],
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

    public function testFullColumnsDefaultValues()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns2);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, zip, gpa, uniform) DEFAULT VALUES");
        $this->assertTrue($result);

        $expected = [ [ 12, 'Jane', 'Doe', 12345, 4.0, 3] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testMissingColumns()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, gpa) VALUES (1, 'John', 'Smith', 3.6);");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', null, 3.6, null ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testFullColumnsFullValues()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, zip, gpa, uniform) VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL');");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testNoColumnsFull()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL');");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

   public function testMultiRow()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, gpa) VALUES (1, 'John', 'Smith', 3.6), (2, 'Jane', 'Doe', 4.0 );");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', null, 3.6, null ], [ 2, 'Jane', 'Doe', null, 4.0, null ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testMultiRowNoColumns()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL'), (2, 'Jane', 'Doe', 54321, 4.0, 'S');");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ], [ 2, 'Jane', 'Doe', 54321, 4.0, 1 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testSetColumns()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students SET id=1, firstName='John', lastName='Smith', gpa=3.6");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', null, 3.6, null ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testSetColumnsFull()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students SET id=1, firstName='John', lastName='Smith', zip=90210, gpa=3.6, uniform='XL'");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testSelectColumnsNone()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students SELECT 1, 'John', 'Smith', 90210, 3.6, 'XL';");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testSelectColumnsPartial()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, gpa) SELECT 1, 'John', 'Smith', 3.6;");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', null, 3.6, null ]];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testSelectColumnsFull()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, zip, gpa, uniform) SELECT 1, 'John', 'Smith', 90210, 3.6, 'XL';");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testSelectMultiRow()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students (id, firstName, lastName, gpa) VALUES (1, 'John', 'Smith', 3.6), (2, 'Jane', 'Doe', 4.0 );");
        $this->assertTrue($result);

        $table2 = CachedTable::create($this->fsql->current_schema(), 'students2', self::$columns1);
        $result = $this->fsql->query("INSERT INTO students2 SELECT * FROM students;");
        $this->assertTrue($result);

        $expected = [ [ 1, 'John', 'Smith', null, 3.6, null ], [ 2, 'Jane', 'Doe', null, 4.0, null ] ];
        $this->assertEquals($expected, $table2->getEntries());
    }

    public function testIdentityAuto()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("INSERT INTO students VALUES (AUTO, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);

        $result = $this->fsql->query("INSERT INTO students VALUES (AUTO, 'Arthur', 'Dent', 12345, 2.5);");
        $this->assertTrue($result);

        $expected = [ [ 3, 'John', 'Smith', 90210, 3.6 ], [ 4, 'Arthur', 'Dent', 12345, 2.5 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testIdentityDefault()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("INSERT INTO students VALUES (DEFAULT, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);

        $result = $this->fsql->query("INSERT INTO students VALUES (DEFAULT, 'Arthur', 'Dent', 12345, 2.5);");
        $this->assertTrue($result);

        $expected = [ [ 3, 'John', 'Smith', 90210, 3.6 ], [ 4, 'Arthur', 'Dent', 12345, 2.5 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testIdentityNull()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("INSERT INTO students VALUES (NULL, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);

        $result = $this->fsql->query("INSERT INTO students VALUES (NULL, 'Arthur', 'Dent', 12345, 2.5);");
        $this->assertTrue($result);

        $expected = [ [ 3, 'John', 'Smith', 90210, 3.6 ], [ 4, 'Arthur', 'Dent', 12345, 2.5 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testIdentityManual()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("INSERT INTO students VALUES (AUTO, 'Arthur', 'Dent', 12345, 2.5);");
        $this->assertTrue($result);

        $result = $this->fsql->query("INSERT INTO students VALUES (12, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);

        $result = $this->fsql->query("INSERT INTO students VALUES (AUTO, 'Jane', 'Doe', 54321, 4.0);");
        $this->assertTrue($result);

        $expected = [[ 3, 'Arthur', 'Dent', 12345, 2.5 ], [ 12, 'John', 'Smith', 90210, 3.6 ], [ 4, 'Jane', 'Doe', 54321, 4.0 ] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testIdentityManualAlways()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns4);

        $result = $this->fsql->query("INSERT INTO students VALUES (12, 'John', 'Smith', 90210, 3.6);");
        $this->assertFalse($result);
        $this->assertEquals("Manual value inserted into an ALWAYS identity column", trim($this->fsql->error()));
    }

    public function testPrimaryKeyCollision()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("INSERT INTO students VALUES (12, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);

        $result = $this->fsql->query("INSERT INTO students VALUES (12, 'Jane', 'Doe', 54321, 4.0);");
        $this->assertFalse($result);
        $this->assertEquals("Duplicate value for unique column 'id'", trim($this->fsql->error()));
    }

    public function testPrimaryKeyReplace()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("REPLACE INTO students VALUES (12, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);
        $expected = [ [ 12, 'John', 'Smith', 90210, 3.6 ] ];
        $this->assertEquals($expected, $table->getEntries());

        $result = $this->fsql->query("REPLACE INTO students VALUES (12, 'Jane', 'Doe', 54321, 4.0);");
        $this->assertTrue($result);

        $expected = [ [ 12, 'Jane', 'Doe', 54321, 4.0 ] ];
        $this->assertEquals($expected, array_values($table->getEntries()));
    }

    public function testPrimaryKeyIgnore()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns3);

        $result = $this->fsql->query("INSERT IGNORE INTO students VALUES (12, 'John', 'Smith', 90210, 3.6);");
        $this->assertTrue($result);
        $expected = [ [ 12, 'John', 'Smith', 90210, 3.6 ] ];
        $this->assertEquals($expected, $table->getEntries());

        $result = $this->fsql->query("INSERT IGNORE INTO students VALUES (12, 'Jane', 'Doe', 54321, 4.0);");
        $this->assertTrue($result);

        $this->assertEquals($expected, $table->getEntries());
    }

    public function testTransactionCommit()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $this->fsql->begin();
        $result = $this->fsql->query("INSERT INTO students VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL');");
        $result = $this->fsql->query("INSERT INTO students VALUES (2, 'Arthur', 'Dent', 12345, 2.5, 'M');");
        $this->fsql->commit();

        $expected = [ [ 1, 'John', 'Smith', 90210, 3.6, 4 ], [2, 'Arthur', 'Dent', 12345, 2.5, 2] ];
        $this->assertEquals($expected, $table->getEntries());
    }

    public function testTransactionRollback()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $this->fsql->begin();
        $result = $this->fsql->query("INSERT INTO students VALUES (1, 'John', 'Smith', 90210, 3.6, 'XL');");
        $result = $this->fsql->query("INSERT INTO students VALUES (2, 'Arthur', 'Dent', 12345, 2.5, 'M');");
        $this->fsql->rollback();

        $this->assertSame([], $table->getEntries());
    }
}
