<?php

require_once dirname(__FILE__).'/fSQLBaseTest.php';

class fSQLCachedTablest extends fSQLBaseTest
{
    private static $columns = array(
        'id' => array('type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array(3, 0, 1, 1, 1, 10000, 0)),
        'username' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'age' => array('type' => 'i', 'auto' => 0, 'default' => 18, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'address' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'salary' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'size' => array('type' => 'e', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => array('small', 'medium', 'large')),
    );

    private static $columns2 = array(
        'username' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'email' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
    );

    private static $entries = array(
        array(1, 'bill', 32, '1234 Someplace Lane', 112334.0, 'medium'),
        array(2, 'smith', 27, '1031 Elm Street', 11.11, 'small'),
    );

    private $schema;

    public function setUp()
    {
        parent::setUp();
        $fsql = new fSQLEnvironment();
        $fsql->define_db('db1', parent::$tempDir);
        $this->schema = $fsql->get_database('db1')->getSchema('public');
    }

    public function testConstructor()
    {
        $tableName = 'garbage';
        $table = new fSQLCachedTable($this->schema, $tableName);
        $this->assertEquals($tableName, $table->name());
        $this->assertEquals($this->schema, $table->schema());
    }

    public function testCreate()
    {
        $tableName = 'blah';
        $table = fSQLCachedTable::create($this->schema, $tableName, self::$columns);
        $this->assertNotNull($table);
        $this->assertEquals($tableName, $table->name());
        $this->assertEquals($this->schema, $table->schema());
    }

    public function testFullName()
    {
        $tableName = 'garbage';
        $table = new fSQLCachedTable($this->schema, $tableName);
        $this->assertEquals($this->schema->fullName().'.'.$tableName, $table->fullName());
    }

    public function testExists()
    {
        $table = new fSQLCachedTable($this->schema, 'newTable');
        $this->assertFalse($table->exists());

        $createdTable = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertTrue($createdTable->exists());
    }

    public function testTemporary()
    {
        $table = new fSQLCachedTable($this->schema, 'newTable');
        $this->assertFalse($table->temporary());
    }

    public function testGetColumns()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertEquals($table->getColumns(), self::$columns);
    }

    public function testGetColumnNames()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertEquals($table->getColumnNames(), array_keys(self::$columns));
    }

    public function testSetColumns()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns2);
        $this->assertEquals($table->getColumns(), self::$columns2);

        $table->setColumns(self::$columns);
        $this->assertEquals($table->getColumns(), self::$columns);
    }

    public function testGetEntriesEmpty()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertEmpty($table->getEntries());
    }

    public function testInsertRow()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->insertRow(self::$entries[0]);
        $table->insertRow(self::$entries[1]);

        $this->assertEquals($table->getEntries(), self::$entries);
    }

    public function testUpdateRow()
    {
        $update = array(1 => 'jsmith', 4 => 10000000.0);

        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->insertRow(self::$entries[0]);
        $table->insertRow(self::$entries[1]);

        $table->updateRow(1, $update);
        $entries = $table->getEntries();
        $this->assertEquals(array(2, 'jsmith', 27, '1031 Elm Street', 10000000.0, 'small'), $entries[1]);
    }

    public function testDeleteRow()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->insertRow(self::$entries[0]);
        $table->insertRow(self::$entries[1]);

        $table->deleteRow(0);
        $this->assertEquals($table->getEntries(), array(1 => self::$entries[1]));
    }

    public function testRollback()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->insertRow(self::$entries[0]);
        $table->insertRow(self::$entries[1]);
        $this->assertEquals($table->getEntries(), self::$entries);

        $table->rollback();
        $this->assertEmpty($table->getEntries());
    }

    public function testCommitNothing()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->commit();
    }

    public function testCommit()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->insertRow(self::$entries[0]);
        $table->commit();

        $table->insertRow(self::$entries[1]);
        $table->rollback();
        $this->assertEquals(count($table->getEntries()), 1);
    }

    public function testDrop()
    {
        $tableName = 'blah';
        $table = fSQLCachedTable::create($this->schema, $tableName, self::$columns);
        $dataFile = $table->dataFile->getPath();
        $table->drop();
        $this->assertFalse(file_exists($dataFile));
    }

    public function testTruncate()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->insertRow(self::$entries[0]);
        $table->insertRow(self::$entries[1]);
        $table->commit();
        $this->assertEquals(count($table->getEntries()), 2);

        $table->truncate();
        $this->assertTrue($table->exists());
        $this->assertEmpty($table->getEntries());
    }

    public function testGetIdentityNone()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns2);
        $identity = $table->getIdentity();
        $this->assertNull($identity);
    }

    public function testGetIdentity()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $identity = $table->getIdentity();
        $this->assertNotNull($identity);
        $this->assertInstanceOf('fSQLIdentity', $identity);
    }

    public function testDropIdentityNone()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->dropIdentity();
        $this->assertNull($table->getIdentity());
    }

    public function testDropIdentity()
    {
        $table = fSQLCachedTable::create($this->schema, 'blah', self::$columns);
        $table->dropIdentity();
        $this->assertNull($table->getIdentity());

        $columns = $table->getColumns();
        $this->assertEmpty($columns['id']['restraint']);
    }
}
