<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\SequencesFile;
use FSQL\Environment;

class DropSchemaTest extends BaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testDropSchemaDbNoExist()
    {
        $result = $this->fsql->query("DROP SCHEMA wrongDB.db1;");
        $this->assertFalse($result);
        $this->assertEquals("Database wrongDB not found", trim($this->fsql->error()));
    }

    public function testDropSchemaNoExist()
    {
        $result = $this->fsql->query("DROP SCHEMA blah;");
        $this->assertFalse($result);
        $this->assertEquals("Schema db1.blah does not exist", trim($this->fsql->error()));
    }

    public function testDropSchemaIfExistsNoExist()
    {
        $result = $this->fsql->query("DROP SCHEMA IF EXISTS blah;");
        $this->assertTrue($result);
    }

    public function testDropSchema()
    {
        $this->fsql->query('CREATE SCHEMA mySchema');
        $result = $this->fsql->query("DROP SCHEMA mySchema;");
        $this->assertTrue($result);
    }

    public function testDropSchemaMultiple()
    {
        $this->fsql->query('CREATE SCHEMA schema1');
        $this->fsql->query('CREATE SCHEMA schema2');
        $this->fsql->query('CREATE SCHEMA schema3');
        $result = $this->fsql->query("DROP SCHEMA schema1, schema2, schema3;");
        $this->assertTrue($result);
    }
}