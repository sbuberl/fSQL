<?php

require_once dirname(__FILE__).'/fSQLBaseTest.php';

class fSQLDatabaseTest extends fSQLBaseTest
{
    private static $columns = array(
        'id' => array('type' => 'i', 'auto' => '0', 'default' => 0, 'key' => 'p', 'null' => '0', 'restraint' => array()),
        'username' => array('type' => 's', 'auto' => '0', 'default' => '', 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'age' => array('type' => 'i', 'auto' => '0', 'default' => 18, 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'address' => array('type' => 's', 'auto' => '0', 'default' => '', 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'salary' => array('type' => 'f', 'auto' => '0', 'default' => 0.0, 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'size' => array('type' => 'e', 'auto' => '0', 'default' => 0.0, 'key' => 'n', 'null' => '1', 'restraint' => array('small', 'medium', 'large')),
    );

    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
    }

    public function testConstructor()
    {
        $name = 'shazam';
        $path = 'blah/blah';
        $db = new fSQLDatabase($this->fsql, $name, $path);

        $this->assertEquals($name, $db->name());
        $this->assertEquals($path, $db->path());
        $this->assertEquals($this->fsql, $db->environment());
    }

    public function testDefineSchema()
    {
        $name = 'mySchema';
        $db = new fSQLDatabase($this->fsql, 'db1', parent::$tempDir);
        $db->create();

        $passed = $db->defineSchema($name);
        $this->assertTrue($passed);
        $this->assertEquals($name, $db->getSchema($name)->name());
    }

    public function testListSchemas()
    {
        $db = new fSQLDatabase($this->fsql, 'shazam', parent::$tempDir);
        $db->create();

        $db->defineSchema('schema1');
        $db->defineSchema('schema2');

        $schemas = $db->listSchemas();
        $this->assertEquals(array('public', 'schema1', 'schema2'), $schemas);
    }

    public function testDrop()
    {
        $db = new fSQLDatabase($this->fsql, 'shazam', parent::$tempDir);
        $db->create();

        $schema = $db->defineSchema('testing');
        $schema2 = $db->defineSchema('stuff');
        $this->assertNotEmpty($db->listSchemas());

        $db->drop();
        $this->assertEquals(array('public'), $db->listSchemas());
    }

    public function GetSchemaNonExist()
    {
        $db = new fSQLDatabase($this->fsql, 'shazam', parent::$tempDir);
        $db->create();

        $schema = $db->getSchema('blah');
        $this->assertFalse($schema);
    }

    public function testGetSchema()
    {
        $name = 'schemaA';
        $db = new fSQLDatabase($this->fsql, 'shazam', parent::$tempDir);
        $db->create();
        $db->defineSchema($name);

        $schema = $db->getSchema($name);
        $this->assertInstanceOf('fSQLSchema', $schema);
    }
}
