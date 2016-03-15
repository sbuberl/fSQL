<?php

require_once dirname(__FILE__).'/fSQLBaseTest.php';

class CreateSchemaTest extends fSQLBaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testDBNotFound()
    {
        $dbName = 'wrongDB';
        $result = $this->fsql->query("CREATE SCHEMA $dbName.mySchema;");
        $this->assertFalse($result);
        $this->assertEquals("Database $dbName not found", trim($this->fsql->error()));
    }

    public function testFoundError()
    {
        $fullName = 'db1.public';
        $result = $this->fsql->query("CREATE SCHEMA $fullName;");
        $this->assertFalse($result);
        $this->assertEquals("Schema $fullName already exists", trim($this->fsql->error()));
    }

    public function testFoundIgnore()
    {
        $fullName = 'db1.public';
        $result = $this->fsql->query("CREATE SCHEMA IF NOT EXISTS $fullName");
        $this->assertTrue($result);
        $this->assertTrue($this->fsql->current_db()->getSchema('public') !== false);
    }

    public function testSuccess()
    {
        $result = $this->fsql->query('CREATE SCHEMA other');
        $this->assertTrue($result);

        $schema = $this->fsql->current_db()->getSchema('other');
        $this->assertTrue($this->fsql->current_db()->getSchema('public') !== false);
    }
}
