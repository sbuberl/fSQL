<?php

require_once __DIR__.'/SequenceBaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Database\Identity;
use FSQL\Environment;

class IdentityTest extends SequenceBaseTest
{
    private static $columns = array(
        'id' => array('type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => array(3, 0, 1, 1, 1, 10000, 0)),
        'username' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'age' => array('type' => 'i', 'auto' => 0, 'default' => 18, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'address' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'salary' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'size' => array('type' => 'e', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => array('small', 'medium', 'large')),
    );

    public function setUp()
    {
        parent::setUp();
        $fsql = new Environment();
        $fsql->define_db('db1', parent::$tempDir);
        $schema = $fsql->get_database('db1')->getSchema('public');
        $table = CachedTable::create($schema, 'blah', self::$columns);
        $this->sequence = new Identity($table, 'id');
    }

    public function testGetColumnName()
    {
        $this->assertEquals('id', $this->sequence->getColumnName());
    }

    public function testGetAlways()
    {
        $this->assertEquals(0, $this->sequence->getAlways());
    }

    public function testAlterAlways()
    {
        $this->sequence->alter(array('ALWAYS' => 1));

        $this->assertEquals(0, $this->sequence->getAlways());
    }
}
