<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\SequencesFile;
use FSQL\Environment;

class DropDatabaseTest extends BaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testDropNoExist()
    {
        $dbName = 'wrongDB';
        $result = $this->fsql->query("DROP DATABASE $dbName;");
        $this->assertFalse($result);
        $this->assertEquals("Database $dbName not found", trim($this->fsql->error()));
    }

    public function testDropIfExistsNoExist()
    {
        $dbName = 'wrongDB';
        $result = $this->fsql->query("DROP DATABASE IF EXISTS $dbName;");
        $this->assertTrue($result);;
    }

    public function testDrop()
    {
        $result = $this->fsql->query("DROP DATABASE db1;");
        $this->assertTrue($result);
    }

    public function testDropMultiple()
    {
        $this->fsql->define_db('db2', parent::$tempDir."db2");
        $this->fsql->define_db('db3', parent::$tempDir."db2");
        $result = $this->fsql->query("DROP DATABASE db1, db2, db3;");
        $this->assertTrue($result);
    }
}