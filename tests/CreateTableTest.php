<?php

require_once __DIR__.'/BaseTest.php';

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

    public function testLikeTableNotxExist()
    {
        $result = $this->fsql->query('CREATE TABLE people LIKE students;');
        $this->assertFalse($result);
        $this->assertEquals('Relation db1.public.students does not exist', trim($this->fsql->error()));
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
