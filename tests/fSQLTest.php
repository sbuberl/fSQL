<?php

require_once dirname(__FILE__).'/fSQLBaseTest.php';

class fSQLTest extends fSQLBaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
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

    public function testDefineSchema()
    {
        $dbName = 'db1';
        $passed = $this->fsql->define_db($dbName, parent::$tempDir);
        $this->assertTrue($passed);

        $schema1 = 'stuff';
        $passed = $this->fsql->define_schema($dbName, $schema1);
        $this->assertTrue($passed);

        $schema2 = 'junk';
        $passed = $this->fsql->define_schema($dbName, $schema2);
        $this->assertTrue($passed);

        $database = $this->fsql->get_database($dbName);
        $this->assertTrue($database->getSchema($schema1) !== false);
        $this->assertTrue($database->getSchema($schema2) !== false);
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
        $this->fsql->define_schema($dbName, $goodSchema);

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
