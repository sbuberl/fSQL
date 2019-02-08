<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Environment;

class EnvironmentTest extends BaseTest
{
    private $fsql;

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
}
