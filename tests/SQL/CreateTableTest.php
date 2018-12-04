<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class CreateTableTest extends BaseTest
{
    private $fsql;

    private static $columns1 = array(
        'id' => array('type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array(3, 0, 1, 1, 1, 10000, 0)),
        'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
        'lastName' => array('type' => 's', 'auto' => 0, 'default' => 'blah', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'zip' => array('type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 11000000.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
    );

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testAlreadyExists()
    {
        $schema = $this->fsql->current_schema();

        CachedTable::create($schema, "students", self::$columns1);

        $result = $this->fsql->query("CREATE TABLE students (id INTEGER, firstName TEXT, lastName TEXT, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'))");
        $this->assertFalse($result);
        $this->assertEquals('Relation db1.public.students already exists', trim($this->fsql->error()));
    }

    public function testIfNotExists()
    {
        $schema = $this->fsql->current_schema();

        CachedTable::create($schema, "students", self::$columns1);

        $result = $this->fsql->query("CREATE TABLE IF NOT EXISTS students (id INTEGER, firstName TEXT, lastName TEXT, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'))");
        $this->assertTrue($result);;
    }

    public function testColumnNameRepeat()
    {
        $result = $this->fsql->query("CREATE TABLE students (id INTEGER, firstName TEXT, lastName TEXT, zip INT, gpa DOUBLE, id VARCHAR, uniform ENUM('S','M','L','XL'))");
        $this->assertFalse($result);
        $this->assertEquals("Column 'id' redefined", trim($this->fsql->error()));
    }

    public function testUnsupportedType()
    {
        $result = $this->fsql->query("CREATE TABLE students (id SET('a','b','c'), firstName TEXT, lastName TEXT, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'))");
        $this->assertFalse($result);
        $this->assertEquals("Column 'id' has unknown type 'SET'", trim($this->fsql->error()));
    }

    public function testTypesOnly()
    {
        $result = $this->fsql->query("CREATE TABLE students (id INTEGER, firstName TEXT, lastName TEXT, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'))");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('students');
        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'uniform' => array('type' => 'e', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array('S','M','L','XL')),
        );
        $this->assertEquals($expected, $table->getColumns());
        $this->assertFalse($table->temporary());
    }

    public function testTemp()
    {
        $result = $this->fsql->query("CREATE TEMPORARY TABLE students (id INTEGER, firstName TEXT, lastName TEXT, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'))");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('students');
        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'uniform' => array('type' => 'e', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array('S','M','L','XL')),
        );

        $taleColumns = $table->getColumns();

        $this->assertEquals($expected, $table->getColumns());
        $this->assertTrue($table->temporary());
    }

    public function testNotNullNoDefaults()
    {
        $result = $this->fsql->query("CREATE TABLE students (id INTEGER NOT NULL, firstName TEXT NOT NULL, lastName TEXT NOT NULL, zip INT NOT NULL, gpa DOUBLE NOT NULL, uniform ENUM('S','M','L','XL') NOT NULL)");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('students');
        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'uniform' => array('type' => 'e', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array('S','M','L','XL')),
        );
        $this->assertEquals($expected, $table->getColumns());
    }

    public function testNotNullDefaults()
    {
        $result = $this->fsql->query("CREATE TABLE students (id INTEGER NOT NULL DEFAULT 1, firstName TEXT NOT NULL DEFAULT 'John', lastName TEXT NOT NULL DEFAULT 'Smith', zip INT NOT NULL DEFAULT 90210, gpa DOUBLE NOT NULL DEFAULT 4.0, uniform ENUM('S','M','L','XL') NOT NULL DEFAULT 'M')");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('students');
        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => 1, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => 'John', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => 'Smith', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => 90210, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 4.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'uniform' => array('type' => 'e', 'auto' => 0, 'default' => 2, 'key' => 'n', 'null' => 0, 'restraint' => array('S','M','L','XL')),
        );
        $this->assertEquals($expected, $table->getColumns());
    }

    public function testKeysInColumns()
    {
        $result = $this->fsql->query("CREATE TABLE people (id INTEGER NOT NULL PRIMARY KEY, firstName TEXT, lastName TEXT UNIQUE KEY, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'))");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'p', 'null' => 0, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'u', 'null' => 1, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'uniform' => array('type' => 'e', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array('S','M','L','XL')),
        );
        $this->assertEquals($expected, $table->getColumns());
    }

    public function testConstraintRows()
    {
        $result = $this->fsql->query("CREATE TABLE people (id INTEGER NOT NULL, firstName TEXT, lastName TEXT UNIQUE KEY, zip INT, gpa DOUBLE, uniform ENUM('S','M','L','XL'), PRIMARY KEY(id), UNIQUE(lastName))");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'p', 'null' => 0, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'u', 'null' => 1, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'uniform' => array('type' => 'e', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array('S','M','L','XL')),
        );
        $this->assertEquals($expected, $table->getColumns());
    }

    public function testAutoIncrement()
    {
        $result = $this->fsql->query('CREATE TABLE people (id INTEGER AUTO_INCREMENT PRIMARY KEY, firstName TEXT NOT NULL, lastName TEXT NOT NULL, zip INT, gpa FLOAT)');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $identity = $table->getIdentity();
        $this->assertEquals(1, $identity->start);
        $this->assertEquals(1, $identity->current);
        $this->assertEquals(1, $identity->increment);
        $this->assertEquals(1, $identity->min);
        $this->assertEquals(PHP_INT_MAX, $identity->max);
        $this->assertEquals(0, $identity->cycle);
        $this->assertEquals(0, $identity->getAlways());
        $this->assertEquals('id', $identity->getColumnName());
    }

    public function testIdentityAlwaysNoValues()
    {
        $result = $this->fsql->query('CREATE TABLE people (id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, firstName TEXT NOT NULL, lastName TEXT NOT NULL, zip INT, gpa FLOAT)');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $identity = $table->getIdentity();
        $this->assertEquals(1, $identity->start);
        $this->assertEquals(1, $identity->current);
        $this->assertEquals(1, $identity->increment);
        $this->assertEquals(1, $identity->min);
        $this->assertEquals(PHP_INT_MAX, $identity->max);
        $this->assertEquals(0, $identity->cycle);
        $this->assertEquals(1, $identity->getAlways());
        $this->assertEquals('id', $identity->getColumnName());
    }

    public function testIdentityAlways()
    {
        $result = $this->fsql->query('CREATE TABLE people (id INTEGER GENERATED ALWAYS AS IDENTITY(START WITH 7, INCREMENT BY 2, MINVALUE 3, MAXVALUE 10000, CYCLE) PRIMARY KEY, firstName TEXT NOT NULL, lastName TEXT NOT NULL, zip INT, gpa FLOAT)');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $identity = $table->getIdentity();
        $this->assertEquals(7, $identity->start);
        $this->assertEquals(7, $identity->current);
        $this->assertEquals(2, $identity->increment);
        $this->assertEquals(3, $identity->min);
        $this->assertEquals(10000, $identity->max);
        $this->assertEquals(1, $identity->cycle);
        $this->assertEquals(1, $identity->getAlways());
        $this->assertEquals('id', $identity->getColumnName());
    }

    public function testIdentityByDefaultNoValues()
    {
        $result = $this->fsql->query('CREATE TABLE people (id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, firstName TEXT NOT NULL, lastName TEXT NOT NULL, zip INT, gpa FLOAT)');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $identity = $table->getIdentity();
        $this->assertEquals(1, $identity->start);
        $this->assertEquals(1, $identity->current);
        $this->assertEquals(1, $identity->increment);
        $this->assertEquals(1, $identity->min);
        $this->assertEquals(PHP_INT_MAX, $identity->max);
        $this->assertEquals(0, $identity->cycle);
        $this->assertEquals(0, $identity->getAlways());
        $this->assertEquals('id', $identity->getColumnName());
    }

    public function testIdentityByDefault()
    {
        $result = $this->fsql->query('CREATE TABLE people (id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 7, INCREMENT BY 2, MINVALUE 3, MAXVALUE 10000, CYCLE) PRIMARY KEY, firstName TEXT NOT NULL, lastName TEXT NOT NULL, zip INT, gpa FLOAT)');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');
        $identity = $table->getIdentity();
        $this->assertEquals(7, $identity->start);
        $this->assertEquals(7, $identity->current);
        $this->assertEquals(2, $identity->increment);
        $this->assertEquals(3, $identity->min);
        $this->assertEquals(10000, $identity->max);
        $this->assertEquals(1, $identity->cycle);
        $this->assertEquals(0, $identity->getAlways());
        $this->assertEquals('id', $identity->getColumnName());
    }

    public function testLikeTableNotExist()
    {
        $result = $this->fsql->query('CREATE TABLE people LIKE students;');
        $this->assertFalse($result);
        $this->assertEquals('Table db1.public.students does not exist', trim($this->fsql->error()));
    }

    public function testLike()
    {
        $original = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $this->assertTrue($original !== false);

        $result = $this->fsql->query('CREATE TABLE people LIKE students;');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');

        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        );

        $this->assertEquals($expected, $table->getColumns());
    }

    public function testLikeExcluding()
    {
        $original = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $this->assertTrue($original !== false);

        $result = $this->fsql->query('CREATE TABLE people LIKE students EXCLUDING IDENTITY EXCLUDING DEFAULTS');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');

        $expected = array(
            'id' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array()),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        );

        $this->assertEquals($expected, $table->getColumns());
    }

    public function testLikeIncluding()
    {
        $original = CachedTable::create($this->fsql->current_schema(), 'students', self::$columns1);
        $this->assertTrue($original !== false);

        $result = $this->fsql->query('CREATE TABLE people LIKE students INCLUDING IDENTITY INCLUDING DEFAULTS;');
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('people');

        $expected = array(
            'id' => array('type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array(1, 0, 1, 1, 1, 10000, 0)),
            'firstName' => array('type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => array()),
            'lastName' => array('type' => 's', 'auto' => 0, 'default' => 'blah', 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'zip' => array('type' => 'i', 'auto' => 0, 'default' => 12345, 'key' => 'n', 'null' => 0, 'restraint' => array()),
            'gpa' => array('type' => 'f', 'auto' => 0, 'default' => 11000000.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        );

        $this->assertEquals($expected, $table->getColumns());
    }
}
