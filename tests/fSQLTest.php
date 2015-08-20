<?php

require dirname(__FILE__) . '../fSQL.php';

class fSQLTest extends PHPUnit_Framework_TestCase
{
    var $fsql;

    function setUp()
    {
        $this->fsql =& new fSQLEnvironment();
        mkdir(".tmp");
    }

    function tearDown()
    {
        $this->close();
        unset($this->fsql);
        rmdir (".tmp");
    }

    function testDefineDB()
    {
        $this->assertTrue($this->define_db("db1", ".tmp"));
    }
}

?>
