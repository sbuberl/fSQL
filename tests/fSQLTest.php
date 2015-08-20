<?php

require dirname(__FILE__) . '/../fSQL.php';

class fSQLTest extends PHPUnit_Framework_TestCase
{
    var $fsql;

    static $tempDir = ".tmp";

    static function setUpBeforeClass()
    {
        mkdir(self::$tempDir);
    }

    static function tearDownAfterClass()
    {
        rmdir (self::$tempDir);
    }

    function setUp()
    {
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
        $passed = $this->fsql->define_db($dbName, self::$tempDir);
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
        $this->fsql->define_db($dbName, self::$tempDir);

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
