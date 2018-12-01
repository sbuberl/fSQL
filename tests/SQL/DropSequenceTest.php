<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Environment;

class DropSequenceTest extends BaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testDropSequenceDbNoExist()
    {
        $result = $this->fsql->query("DROP SEQUENCE wrongDB.db1.table;");
        $this->assertFalse($result);
        $this->assertEquals("Database wrongDB not found", trim($this->fsql->error()));
    }

    public function testDropSequenceSchemaNoExist()
    {
        $result = $this->fsql->query("DROP SEQUENCE schema2.table;");
        $this->assertFalse($result);
        $this->assertEquals("Schema db1.schema2 does not exist", trim($this->fsql->error()));
    }

    public function testDropSequenceNoExist()
    {
        $result = $this->fsql->query("DROP SEQUENCE blah;");
        $this->assertFalse($result);
        $this->assertEquals("Sequence db1.public.blah does not exist", trim($this->fsql->error()));
    }

    public function testDropSequenceIfExistsNoExist()
    {
        $result = $this->fsql->query("DROP SEQUENCE IF EXISTS blah;");
        $this->assertTrue($result);
    }

    public function testDropSequence()
    {
        $this->fsql->query('CREATE SEQUENCE userids INCREMENT BY 2 START WITH 22 MAXVALUE 66 NO CYCLE');
        $result = $this->fsql->query("DROP SEQUENCE userids;");
        $this->assertTrue($result);
    }

    public function testDropSequenceMultiple()
    {
        $this->fsql->query('CREATE SEQUENCE userids1 INCREMENT BY 2 START WITH 22 MAXVALUE 66 NO CYCLE');
        $this->fsql->query('CREATE SEQUENCE userids2 INCREMENT BY 10 START WITH 1 CYCLE');
        $this->fsql->query('CREATE SEQUENCE userids3 INCREMENT BY 1 START WITH 100000 MAXVALUE 100000000000 CYCLE');
        $result = $this->fsql->query("DROP SEQUENCE userids1, userids2, userids3;");
        $this->assertTrue($result);
    }
}