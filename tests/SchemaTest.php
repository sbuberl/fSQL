<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\Schema;
use FSQL\Environment;

class SchemaTest extends BaseTest
{
    private static $columns = array(
        'id' => array('type' => 'i', 'auto' => '0', 'default' => 0, 'key' => 'p', 'null' => '0', 'restraint' => array()),
        'username' => array('type' => 's', 'auto' => '0', 'default' => '', 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'age' => array('type' => 'i', 'auto' => '0', 'default' => 18, 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'address' => array('type' => 's', 'auto' => '0', 'default' => '', 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'salary' => array('type' => 'f', 'auto' => '0', 'default' => 0.0, 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'size' => array('type' => 'e', 'auto' => '0', 'default' => 0.0, 'key' => 'n', 'null' => '1', 'restraint' => array('small', 'medium', 'large')),
    );

    private $subDir;
    private $db;

    public function setUp()
    {
        parent::setUp();
        $this->subDir = parent::$tempDir.'sub/';
        mkdir($this->subDir);
        $fsql = new Environment();
        $fsql->define_db('db1', parent::$tempDir);
        $this->db = $fsql->get_database('db1');
    }

    public function testConstructorPublic()
    {
        $name = 'shazam';
        $path = 'blah/blah';
        $schema = new Schema($this->db, 'public');

        $this->assertEquals('public', $schema->name());
        $this->assertEquals($this->db->path(), $schema->path());
        $this->assertEquals($this->db, $schema->database());
    }

    public function testConstructor()
    {
        $schema = new Schema($this->db, 'myschema');

        $this->assertEquals('myschema', $schema->name());
        $this->assertEquals($this->db->path().'myschema/', $schema->path());
        $this->assertEquals($this->db, $schema->database());
    }

    public function testCreateFailed()
    {
        touch(parent::$tempDir.'myschema');

        $schema = new Schema($this->db, 'myschema');

        $passed = $schema->create();
        $this->assertFalse($passed);
    }

    public function testCreateTable()
    {
        $name = 'customers';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $table = $schema->createTable($name, self::$columns, false);
        $this->assertFalse($table->temporary());
        $this->assertEquals($name, $table->name());
    }

    public function testCreateTableTemp()
    {
        $name = 'customers';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $table = $schema->createTable($name, self::$columns, true);
        $this->assertTrue($table->temporary());
        $this->assertEquals($name, $table->name());
    }

    public function testGetSequences()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $sequences = $schema->getSequences();
        $this->assertNotNull($sequences);
        $this->assertInstanceOf('FSQL\Database\SequencesFile', $sequences);
    }

    public function testListTablesNone()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $tables = $schema->listTables();
        $this->assertEmpty($tables);
    }

    // skips temp tables like mySQL does
    public function testListTables()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $schema->createTable('temp1', self::$columns, true);
        $schema->createTable('temp2', self::$columns, true);
        $schema->createTable('real1', self::$columns, false);
        $schema->createTable('real2', self::$columns, false);

        $tables = $schema->listTables();
        $this->assertEquals(array('real1', 'real2'), $tables);
    }

    public function testDrop()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $table = $schema->createTable('blah', self::$columns, false);
        $sequences = $schema->getSequences();
        $sequences->create();
        $this->assertNotEmpty($schema->listTables());
        $this->assertTrue($sequences->exists());

        $schema->drop();
        $this->assertFalse($sequences->exists());
        $this->assertEmpty($schema->listTables());
    }

    public function testGetTableEmpty()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $table = $schema->getTable('myTable');
        $this->assertFalse($table->temporary());
        $this->assertEquals('myTable', $table->name());
    }

    public function testGetTableTemp()
    {
        $name = 'temp1';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();
        $schema->createTable($name, self::$columns, true);

        $table = $schema->getTable($name);
        $this->assertTrue($table->temporary());
        $this->assertEquals($name, $table->name());
    }

    public function testGetTable()
    {
        $name = 'table1';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $schema->createTable($name, self::$columns, false);
        $schema->createTable('table2', self::$columns, false);

        $table = $schema->getTable($name);
        $this->assertInstanceOf('FSQL\Database\CachedTable', $table);
        $this->assertEquals($name, $table->name());
    }

    public function testRenameDoesntExist()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $passed = $schema->renameTable('answer42', 'else', $schema);
        $this->assertFalse($passed);
    }

    public function testRename()
    {
        $from = 'blah';
        $to = 'else';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();
        $schema->createTable($from, self::$columns, false);

        $passed = $schema->renameTable($from, $to, $schema);
        $this->assertTrue($passed);
        $newTable = $schema->getTable($to);
        $this->assertTrue($newTable->exists());
        $oldTable = $schema->getTable($from);
        $this->assertFalse($oldTable->exists());
    }

    public function testRenameTemp()
    {
        $from = 'blah';
        $to = 'else';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $schema->createTable($from, self::$columns, true);
        $passed = $schema->renameTable($from, $to, $schema);
        $this->assertTrue($passed);
        $newTable = $schema->getTable($to);
        $this->assertTrue($newTable->exists());
        $oldTable = $schema->getTable($from);
        $this->assertFalse($oldTable->exists());
    }

    public function testRenameToOtherSchema()
    {
        $from = 'temp1';
        $to = 'something';

        $schema = new Schema($this->db, 'myschema');
        $schema->create();
        $schema->createTable($from, self::$columns, true);

        $schema2 = new Schema($this->db, 'other');
        $schema2->create();

        $passed = $schema->renameTable($from, $to, $schema2);
        $this->assertTrue($passed);
        $newTable = $schema2->getTable($to);
        $this->assertTrue($newTable->exists());
        $oldTable = $schema->getTable($from);
        $this->assertFalse($oldTable->exists());
    }

    public function testDropTableDoesntExist()
    {
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $passed = $schema->dropTable('answer42');
        $this->assertFalse($passed);
    }

    public function testDropTable()
    {
        $name = 'blah';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();

        $schema->createTable($name, self::$columns, false);
        $passed = $schema->dropTable($name);
        $this->assertTrue($passed);
        $table = $schema->getTable($name);
        $this->assertFalse($table->exists());
    }

    public function testDropTableTemp()
    {
        $name = 'blah';
        $schema = new Schema($this->db, 'myschema');
        $schema->create();
        $schema->createTable($name, self::$columns, true);
        $passed = $schema->dropTable($name);
        $this->assertTrue($passed);
        $table = $schema->getTable($name);
        $this->assertFalse($table->exists());
    }
}
