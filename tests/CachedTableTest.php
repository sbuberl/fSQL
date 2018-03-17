<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Database\TempTable;
use FSQL\Environment;

class CachedTablest extends BaseTest
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

    private static $columns3 = array(
        'id' => array('type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array()),
        'username' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'age' => array('type' => 'i', 'auto' => 0, 'default' => 18, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'address' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'salary' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'size' => array('type' => 'e', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => array('small', 'medium', 'large')),
    );

    private static $entries = array(
        array(1, 'bill', 32, '1234 Someplace Lane', 112334.0, 'medium'),
        array(2, 'smith', 27, '1031 Elm Street', 11.11, 'small'),
    );

    private $schema;

    public function setUp()
    {
        parent::setUp();
        $fsql = new Environment();
        $fsql->define_db('db1', parent::$tempDir);
        $this->schema = $fsql->get_database('db1')->getSchema('public');
    }

    public function testConstructor()
    {
        $tableName = 'garbage';
        $table = new CachedTable($this->schema, $tableName);
        $this->assertEquals($tableName, $table->name());
        $this->assertEquals($this->schema, $table->schema());
    }

    public function testCreate()
    {
        $tableName = 'blah';
        $table = CachedTable::create($this->schema, $tableName, self::$columns);
        $this->assertNotNull($table);
        $this->assertEquals($tableName, $table->name());
        $this->assertEquals($this->schema, $table->schema());
    }

    public function testCreateTemp()
    {
        $tableName = 'blah';
        $table = TempTable::create($this->schema, $tableName, self::$columns);
        $this->assertNotNull($table);
        $this->assertEquals($tableName, $table->name());
        $this->assertEquals($this->schema, $table->schema());
    }

    public function testFullName()
    {
        $tableName = 'garbage';
        $table = new CachedTable($this->schema, $tableName);
        $this->assertEquals($this->schema->fullName().'.'.$tableName, $table->fullName());
    }

    public function testExists()
    {
        $table = new CachedTable($this->schema, 'newTable');
        $this->assertFalse($table->exists());

        $createdTable = CachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertTrue($createdTable->exists());

        $tempTable = TempTable::create($this->schema, 'blah2', self::$columns);
        $this->assertTrue($tempTable->exists());
    }

    public function testTemporary()
    {
        $table = new CachedTable($this->schema, 'newTable');
        $this->assertFalse($table->temporary());

        $tempTable = TempTable::create($this->schema, 'blah2', self::$columns);
        $this->assertTrue($tempTable->temporary());
    }

    public function testGetColumns()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertEquals($table->getColumns(), self::$columns);
    }

    public function testGetColumnNames()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertEquals($table->getColumnNames(), array_keys(self::$columns));
    }

    public function testSetColumns()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns2);
        $this->assertEquals($table->getColumns(), self::$columns2);

        $table->setColumns(self::$columns);
        $this->assertEquals($table->getColumns(), self::$columns);
    }

    public function testGetEntriesEmpty()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $this->assertSame(array(), $table->getEntries());
    }

    public function testRollback()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $cursor = $table->getWriteCursor();
        $cursor->appendRow(self::$entries[0]);
        $cursor->appendRow(self::$entries[1]);
        $this->assertEquals($table->getEntries(), self::$entries);

        $table->rollback();
        $this->assertSame(array(), $table->getEntries());
    }

    public function testCommitNothing()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $table->commit();
    }

    public function testCommit()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $cursor = $table->getWriteCursor();
        $cursor->appendRow(self::$entries[0]);
        $table->commit();

        $cursor->appendRow(self::$entries[1]);
        $table->rollback();
        $this->assertEquals(count($table->getEntries()), 1);
    }

    public function testDrop()
    {
        $tableName = 'blah';
        $table = CachedTable::create($this->schema, $tableName, self::$columns);
        $dataFile = $table->dataFile->getPath();
        $table->drop();
        $this->assertFalse(file_exists($dataFile));
    }

    public function testTruncate()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $cursor = $table->getWriteCursor();
        $cursor->appendRow(self::$entries[0]);
        $cursor->appendRow(self::$entries[1]);
        $table->commit();
        $this->assertEquals(count($table->getEntries()), 2);

        $table->truncate();
        $this->assertTrue($table->exists());
        $this->assertSame(array(), $table->getEntries());
    }

    public function testGetIdentityNone()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns2);
        $identity = $table->getIdentity();
        $this->assertNull($identity);
    }

    public function testGetIdentityUpgrade()
    {
        $table = CachedTable::create($this->schema, 'students', self::$columns3);
        $cursor = $table->getWriteCursor();
        $cursor->appendRow(self::$entries[0]);
        $cursor->appendRow(self::$entries[1]);

        $identity = $table->getIdentity();
        $this->assertNotNull($identity);

        $identity = $table->getIdentity();
        $this->assertEquals($identity->current, 3);
        $this->assertFalse($identity->getAlways());
        $this->assertEquals($identity->start, 1);
        $this->assertEquals($identity->increment, 1);
        $this->assertEquals($identity->min, 1);
        $this->assertEquals($identity->max, PHP_INT_MAX);
        $this->assertFalse($identity->cycle);
    }

    public function testGetIdentity()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $identity = $table->getIdentity();
        $this->assertNotNull($identity);
        $this->assertInstanceOf('FSQL\Database\Identity', $identity);
    }

    public function testDropIdentityNone()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $table->dropIdentity();
        $this->assertNull($table->getIdentity());
    }

    public function testDropIdentity()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $table->dropIdentity();
        $this->assertNull($table->getIdentity());

        $columns = $table->getColumns();
        $this->assertSame(array(), $columns['id']['restraint']);
    }

    public function testGetCursor()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $cursor = $table->getWriteCursor();
        $cursor->appendRow(self::$entries[0]);
        $cursor->appendRow(self::$entries[1]);

        $cursor = $table->getCursor();
        $this->assertInstanceOf('FSQL\Database\TableCursor', $cursor);
        $this->assertTrue($cursor->valid());
        $this->assertEquals(0, $cursor->key());
    }

    public function testNewCursor()
    {
        $table = CachedTable::create($this->schema, 'blah', self::$columns);
        $cursor = $table->getWriteCursor();
        $cursor->appendRow(self::$entries[0]);
        $cursor->appendRow(self::$entries[1]);

        $cursor = $table->newCursor();
        $this->assertInstanceOf('FSQL\Database\TableCursor', $cursor);
        $this->assertTrue($cursor->valid());
        $this->assertEquals(0, $cursor->key());
    }
}
