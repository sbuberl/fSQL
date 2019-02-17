<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class UseTest extends BaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
    }

    public function testSetDBNoDatabase()
    {
        $result = $this->fsql->query('USE `BAM`');
        $this->assertFalse($result);
        $this->assertEquals(trim($this->fsql->error()), "No database called BAM found");

        $currentDb = $this->fsql->current_db();
        $this->assertNull($currentDb);
    }

    public function testSetDBSuccess()
    {
        $this->fsql->define_db('db1', parent::$tempDir);

        $result = $this->fsql->query('USE db1');
        $this->assertTrue($result);

        $currentDb = $this->fsql->current_db();
        $this->assertNotNull($currentDb);
        $this->assertEquals($currentDb->name(), 'db1');
    }
}
