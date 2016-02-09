<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class fSQLTest extends fSQLBaseTest
{
    var $fsql;

    function setUp()
    {
        parent::setUp();
        $this->fsql =& new fSQLEnvironment();
    }

    function tearDown()
    {
        $this->fsql->close();
        unset($this->fsql);
    }

    function testDefineDB()
    {
        $dbName = "db1";
        $passed = $this->fsql->define_db($dbName, parent::$tempDir);
        $this->assertTrue($passed);

        $db2Name = "stuff";
        $passed = $this->fsql->define_db($db2Name, "./");
        $this->assertTrue($passed);

        $this->assertTrue(isset($this->fsql->databases[$dbName]));
        $this->assertTrue(isset($this->fsql->databases[$db2Name]));
    }

    function testSelectDB()
    {
        $dbName = "db";
        $this->fsql->define_db($dbName, parent::$tempDir);

        $fakeDb = "BAM";
        $fakePassed = $this->fsql->select_db($fakeDb);
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "No database called {$fakeDb} found");

        $db1Passed = $this->fsql->select_db($dbName);
        $this->assertTrue($db1Passed);
        $this->assertNotNull($this->fsql->currentDB);
        $this->assertEquals($this->fsql->currentDB->name(), $dbName);

        $fakePassed = $this->fsql->select_db($fakeDb);
        $this->assertFalse($fakePassed);
        $this->assertEquals(trim($this->fsql->error()), "No database called {$fakeDb} found");
        $this->assertEquals($this->fsql->currentDB->name(), $dbName);
    }
}

?>
