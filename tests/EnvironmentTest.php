<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\Statement;

class EnvironmentTest extends BaseTest
{
    private $fsql;

    private static $columns = [
        'personId' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $entries = [
        [1, 'bill', 'smith', 'chicago'],
        [2, 'jon', 'doe', 'baltimore'],
        [3, 'mary', 'shelley', 'seattle'],
        [4, 'stephen', 'king', 'derry'],
        [5, 'bart', 'simpson', 'springfield'],
        [6, 'jane', 'doe', 'seattle'],
        [7, 'bram', 'stoker', 'new york'],
        [8, 'douglas', 'adams', 'london'],
        [9, 'bill', 'johnson', 'derry'],
        [10, 'jon', 'doe', 'new york'],
        [11, 'homer', null, 'boston'],
        [12, null, 'king', 'tokyo'],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
    }

    public function tearDown()
    {
        unset($this->fsql);
    }

    public function testDefineDB()
    {
        $dbName = 'db1';
        $passed = $this->fsql->define_db($dbName, parent::$tempDir);
        $this->assertTrue($passed);

        $db2Name = 'stuff';
        $passed = $this->fsql->define_db($db2Name, './');
        $this->assertTrue($passed);

        $this->assertTrue($this->fsql->get_database($dbName) !== false);
        $this->assertTrue($this->fsql->get_database($db2Name) !== false);
    }

    public function testSelectDB()
    {
        $dbName = 'db';
        $this->fsql->define_db($dbName, parent::$tempDir);

        $fakeDb = 'BAM';
        $fakePassed = $this->fsql->select_db($fakeDb);
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "No database called {$fakeDb} found");

        $db1Passed = $this->fsql->select_db($dbName);
        $this->assertTrue($db1Passed);
        $currentDb = $this->fsql->current_db();
        $this->assertNotNull($currentDb);
        $this->assertEquals($currentDb->name(), $dbName);

        $fakePassed = $this->fsql->select_db($fakeDb);
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "No database called {$fakeDb} found");
        $this->assertEquals($this->fsql->current_db()->name(), $dbName);
    }

    public function testSelectSchema()
    {
        $dbName = 'db';
        $this->fsql->define_db($dbName, parent::$tempDir);

        $fakeDb = 'BAM';
        $fakePassed = $this->fsql->select_schema($fakeDb, 'public');
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "No database called {$fakeDb} found");

        $fakeSchema = 'blah';
        $fakePassed = $this->fsql->select_schema($dbName, $fakeSchema);
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "Schema {$dbName}.{$fakeSchema} does not exist");

        $goodSchema = 'stuff';
        $db = $this->fsql->get_database($dbName);
        $db->defineSchema($goodSchema);

        $goodPassed = $this->fsql->select_schema($dbName, $goodSchema);
        $this->assertTrue($goodPassed);
        $currentSchema = $this->fsql->current_schema();
        $this->assertNotNull($currentSchema);
        $this->assertEquals($goodSchema, $currentSchema->name());
        $this->assertEquals($dbName, $currentSchema->database()->name());

        $fakePassed = $this->fsql->select_schema($dbName, $fakeSchema);
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "Schema {$dbName}.{$fakeSchema} does not exist");
        $this->assertEquals($goodSchema, $this->fsql->current_schema()->name());
    }

    public function testPrepare()
    {
        $dbName = 'db1';
        $passed = $this->fsql->define_db($dbName, parent::$tempDir);
        $this->fsql->select_db($dbName);

        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
        $cursor->appendRow($entry);
        }
        $table->commit();

        $stmt = $this->fsql->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ?");
        $this->assertTrue($stmt instanceof Statement);
    }
}
