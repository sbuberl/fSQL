<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class AlterTableTest extends BaseTest
{
    private $fsql;

    private static $columnsWithKey = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'email' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'address' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'state' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    private static $columnsWithoutKey = [
        'id' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'email' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'address' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'state' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'zip' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    private static $entries = [
        [1, 'George', 'Washington', 'gwashington@usa.gov', '3200 Mt Vernon Hwy', 'Mount Vernon', 'VA', 22121],
        [2, 'John', 'Adams', 'jadams@usa.gov', '1250 Hancock St', 'Quincy', 'MA', 21069],
        [3, 'Thomas', 'Jefferson', 'tjefferson@usa.gov', '931 Thomas Jefferson Pkwy',  'Charlottesville', 'VA', 22902],
        [4, 'James', 'Madison', 'jmadison@usa.gov', '11350 Constitution Hwy',  'Orange', 'VA', 22960],
        [5, 'James', 'Monroe', 'jmonroe@usa.gov', '2050 James Monroe Parkway', 'Charlottesville', 'VA', 22902],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testAlterTableWrongDB()
    {
        $dbName = 'wrongDB';
        $result = $this->fsql->query("ALTER TABLE $dbName.public.students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Database $dbName not found", trim($this->fsql->error()));
    }

    public function testAlterTableWrongSchema()
    {
        $schemaName = 'wrongSchema';
        $result = $this->fsql->query("ALTER TABLE $schemaName.students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Schema db1.$schemaName does not exist", trim($this->fsql->error()));
    }

    public function testAlterTableNoExist()
    {
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Table db1.public.students does not exist", trim($this->fsql->error()));
    }

    public function testAlterTableIfExistsNoExist()
    {
        $result = $this->fsql->query("ALTER TABLE IF EXISTS students ADD PRIMARY KEY(id)");
        $this->assertTrue($result);
    }

    public function testAlterTableAddColumnExists()
    {
        CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $result = $this->fsql->query("ALTER TABLE students ADD COLUMN zip INT");
        $this->assertFalse($result);
        $this->assertEquals("Column zip already exists", trim($this->fsql->error()));
    }

    public function testAlterTableAddColumn()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("ALTER TABLE students ADD COLUMN major TEXT NULL");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertTrue(isset($columns['major']));
        $entries = $table->getEntries();
        foreach($entries as $entry) {
            $this->assertTrue(isset($entry[8]));
        }
    }

    public function testAlterTableAddPrimaryKeyExists()
    {
        CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(id)");
        $this->assertFalse($result);
        $this->assertEquals("Primary key already exists", trim($this->fsql->error()));
    }

    public function testAlterTableAddPrimaryKeyBadColumn()
    {
        CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(blah)");
        $this->assertFalse($result);
        $this->assertEquals("Column named 'blah' does not exist in table 'students'", trim($this->fsql->error()));
    }

    public function testAlterTableAddPrimaryKey()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ADD PRIMARY KEY(id)");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['id']['key'], 'p');
    }

    public function testAlterTableAlterColumnBadColumn()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ALTER COLUMN garbage DROP DEFAULT");
        $this->assertFalse($result);
        $this->assertEquals("Column named garbage does not exist in table students", trim($this->fsql->error()));
    }

    public function testAlterTableAlterColumnSetDataType()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("ALTER TABLE students ALTER COLUMN zip SET DATA TYPE TEXT");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['zip']['type'], 's');
        $entries = $table->getEntries();
        foreach($entries as $entry) {
            $this->assertTrue(is_string($entry[7]));
        }
    }

    public function testAlterTableAlterColumnSetDefault()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ALTER COLUMN zip SET DEFAULT 12345");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['zip']['default'], 12345);
    }

    public function testAlterTableAlterColumnDropDefault()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ALTER COLUMN zip DROP DEFAULT");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['zip']['default'], 0.0);
    }

    public function testAlterTableAlterColumnDropIdentityNotAuto()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ALTER COLUMN firstName DROP IDENTITY");
        $this->assertFalse($result);
        $this->assertEquals('Column firstName is not an identity column', trim($this->fsql->error()));
    }

    public function testAlterTableAlterColumnDropIdentity()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students ALTER COLUMN id DROP IDENTITY");
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertNull($table->getIdentity());
        $this->assertEquals($columns['id']['auto'], 0);
        $this->assertEmpty($columns['id']['restraint']);
    }

    public function testAlterTableAlterColumnIdentityNoAuto()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query('ALTER TABLE students ALTER COLUMN zip RESTART');
        $this->assertFalse($result);
        $this->assertEquals('Column zip is not an identity column', trim($this->fsql->error()));
    }

    public function testAlterTableAlterColumnIdentity()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query('ALTER TABLE students ALTER COLUMN id RESTART WITH 5');
        $this->assertTrue($result);
        $identity = $table->getIdentity();
        $this->assertEquals($identity->current, 5);
    }

    public function testAlterTableDropColumnNotExist()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query("ALTER TABLE students DROP COLUMN major");
        $this->assertFalse($result);
        $this->assertEquals('Column named major does not exist in table students', trim($this->fsql->error()));
    }

    public function testAlterTableDropColumn()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();
        $result = $this->fsql->query("ALTER TABLE students DROP COLUMN zip");
        $this->assertTrue($result);

        $table2 = $this->fsql->current_schema()->getTable('students');
        $columns = $table2->getColumns();
        $entries = $table2->getEntries();
        $this->assertFalse(isset($columns['zip']));
        foreach ($entries as $entry) {
            $this->assertFalse(isset($entry[7]));
        }
    }

    public function testAlterTableDropPrimaryKeyNoKey()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithoutKey);
        $result = $this->fsql->query('ALTER TABLE students DROP PRIMARY KEY');
        $this->assertFalse($result);
        $this->assertEquals('No primary key found', trim($this->fsql->error()));
    }

    public function testAlterTableDropPrimaryKey()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $result = $this->fsql->query('ALTER TABLE students DROP PRIMARY KEY');
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['id']['key'], 'n');
    }

    public function testAlterTableRenameTableExists()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $table2 = CachedTable::create($this->fsql->current_schema(), 'people', self::$columnsWithKey);
        $result = $this->fsql->query('ALTER TABLE students RENAME TO people');
        $this->assertFalse($result);
        $this->assertEquals('Destination table db1.public.people already exists', trim($this->fsql->error()));
    }

    public function testAlterTableRenameTable()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $result = $this->fsql->query('ALTER TABLE students RENAME TO people');
        $this->assertTrue($result);
        $db = $this->fsql->get_database("db1");
        $schema = $db->getSchema('public');
        $people = $schema->getTable('people');
        $this->assertTrue($people->exists());
    }

    public function testAlterTableMultipleActions()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'students', self::$columnsWithKey);
        $result = $this->fsql->query('ALTER TABLE students ALTER COLUMN zip SET DEFAULT 12345, ALTER COLUMN id SET GENERATED ALWAYS RESTART WITH 5');
        $this->assertTrue($result);
        $columns = $table->getColumns();
        $this->assertEquals($columns['zip']['default'], 12345);
        $identity = $table->getIdentity();
        $this->assertEquals($identity->getAlways(), true);
        $this->assertEquals($identity->current, 5);
    }
}
