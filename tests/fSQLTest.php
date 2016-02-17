<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class fSQLTest extends fSQLBaseTest
{
    var $fsql;

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
        $dbName = "db1";
        $passed = $this->fsql->define_db($dbName, parent::$tempDir);
        $this->assertTrue($passed);

        $db2Name = "stuff";
        $passed = $this->fsql->define_db($db2Name, "./");
        $this->assertTrue($passed);

        $this->assertTrue($this->fsql->get_database($dbName) !== false);
        $this->assertTrue($this->fsql->get_database($db2Name) !== false);
    }

    public function testSelectDB()
    {
        $dbName = "db";
        $this->fsql->define_db($dbName, parent::$tempDir);

        $fakeDb = "BAM";
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
}
