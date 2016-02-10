<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class fSQLDatabaseTest extends fSQLBaseTest
{
    static $columns = array(
        'id' => array ('type' => 'i', 'auto' => '0', 'default' => 0, 'key' => 'p', 'null' => '0', 'restraint' => array()),
        'username' => array('type' => 's', 'auto' => '0', 'default' => '', 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'age' => array ('type' => 'i', 'auto' => '0', 'default' => 18, 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'address' => array('type' => 's', 'auto' => '0', 'default' => '', 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'salary' => array('type' => 'f', 'auto' => '0', 'default' => 0.0, 'key' => 'n', 'null' => '0', 'restraint' => array()),
        'size' => array('type' => 'e', 'auto' => '0', 'default' => 0.0, 'key' => 'n', 'null' => '1', 'restraint' => array('small', 'medium', 'large'))
    );

    var $subDir;

    function setUp()
    {
        parent::setUp();
        $this->subDir = parent::$tempDir.'sub/';
        mkdir($this->subDir);
    }

    function testConstructor()
    {
        $name = 'shazam';
        $path = 'blah/blah';
        $db =& new fSQLDatabase($name, $path);

        $this->assertEquals($name, $db->name());
        $this->assertEquals($path, $db->path());
    }

    function testClose()
    {
        $db =& new fSQLDatabase('db1', parent::$tempDir);
        $db->createTable('customers', self::$columns, true);
        $db->close();
        $this->assertEmpty(get_object_vars($db));
    }

    function testCreateTable()
    {
        $name = "customers";
        $db =& new fSQLDatabase('db1', parent::$tempDir);
        $table =& $db->createTable($name, self::$columns, false);
        $this->assertInstanceOf('fSQLCachedTable', $table);
        $this->assertEquals($name, $table->name());
    }

    function testCreateTableTemp()
    {
        $name = "customers";
        $db =& new fSQLDatabase('db1', parent::$tempDir);
        $table =& $db->createTable($name, self::$columns, true);
        $this->assertInstanceOf('fSQLTempTable', $table);
        $this->assertEquals($name, $table->name());
    }

    function testListTablesNone()
    {
        $name = 'shazam';
        $path = 'blah/blah';
        $db =& new fSQLDatabase($name, $path);

        $tables = $db->listTables();
        $this->assertEmpty($tables);
    }

    function testGetSequences()
    {
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $sequences = $db->getSequences();
        $this->assertNotNull($sequences);
        $this->assertInstanceOf('fSQLSequencesFile', $sequences);
    }

    // skips temp tables like mySQL does
    function testListTables()
    {
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable('temp1', self::$columns, true);
        $db->createTable('temp2', self::$columns, true);
        $db->createTable('real1', self::$columns, false);
        $db->createTable('real2', self::$columns, false);

        $tables = $db->listTables();
        $this->assertEquals(array('real1', 'real2'), $tables);
    }

    function testGetTableEmpty()
    {
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $table =& $db->getTable('myTable');
        $this->assertInstanceOf('fSQLCachedTable', $table);
        $this->assertEquals('myTable', $table->name());
    }

    function testGetTableTemp()
    {
        $name = 'temp1';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($name, self::$columns, true);

        $table =& $db->getTable($name);
        $this->assertInstanceOf('fSQLTempTable', $table);
        $this->assertEquals($name, $table->name());
    }

    function testGetTable()
    {
        $name = 'table1';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($name, self::$columns, false);
        $db->createTable('table2', self::$columns, false);

        $table =& $db->getTable($name);
        $this->assertInstanceOf('fSQLCachedTable', $table);
        $this->assertEquals($name, $table->name());
    }

    function testRenameDoesntExist()
    {
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $passed = $db->renameTable('answer42', 'else', $this);
        $this->assertFalse($passed);
    }

    function testRename()
    {
        $from = 'blah';
        $to = 'else';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($from, self::$columns, false);
        $passed = $db->renameTable($from, $to, $db);
        $this->assertTrue($passed);
        $newTable =& $db->getTable($to);
        $this->assertTrue($newTable->exists());
        $oldTable =& $db->getTable($from);
        $this->assertFalse($oldTable->exists());
    }

    function testRenameTemp()
    {
        $from = 'blah';
        $to = 'else';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($from, self::$columns, true);
        $passed = $db->renameTable($from, $to, $db);
        $this->assertTrue($passed);
        $newTable =& $db->getTable($to);
        $this->assertTrue($newTable->exists());
        $oldTable =& $db->getTable($from);
        $this->assertFalse($oldTable->exists());
    }

    function testRenameToOtherDB()
    {
        $from = 'temp1';
        $to = 'something';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($from, self::$columns, true);

        $db2 =& new fSQLDatabase('other', $this->subDir);
        $passed = $db->renameTable($from, $to, $db2);
        $this->assertTrue($passed);
        $newTable =& $db2->getTable($to);
        $this->assertTrue($newTable->exists());
        $oldTable =& $db->getTable($from);
        $this->assertFalse($oldTable->exists());
    }

    function testDropTableDoesntExist()
    {
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $passed = $db->dropTable('answer42');
        $this->assertFalse($passed);
    }

    function testDropTable()
    {
        $name = 'blah';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($name, self::$columns, false);
        $passed = $db->dropTable($name);
        $this->assertTrue($passed);
        $table =& $db->getTable($name);
        $this->assertFalse($table->exists());
    }

    function testDropTableTemp()
    {
        $name = 'blah';
        $db =& new fSQLDatabase('shazam', parent::$tempDir);
        $db->createTable($name, self::$columns, true);
        $passed = $db->dropTable($name);
        $this->assertTrue($passed);
        $table =& $db->getTable($name);
        $this->assertFalse($table->exists());
    }
}

?>
