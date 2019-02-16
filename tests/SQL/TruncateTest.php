S<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class TruncateTest extends BaseTest
{
    private $fsql;

    private static $columns1 = [
        'personId' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $columns2 = [
        'personId' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $entries1 = [
        [1, 'bill', 'smith', 'chicago'],
        [2, 'jon', 'doe', 'baltimore'],
        [3, 'mary', 'shelley', 'seattle'],
        [4, 'stephen', 'king', 'derry'],
        [5, 'bart', 'simpson', 'springfield'],
        [6, 'jane', 'doe', 'seattle'],
        [7, 'bram', 'stoker', 'new york'],
        [8, 'douglas', 'adams', 'london'],
        [9, 'bill', 'johnson', 'derry'],
        [10, 'jon', 'doe', 'new york'],
        [11, 'homer', null, 'boston'],
        [12, null, 'king', 'tokyo'],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testTruncateTableNotExist()
    {
        $result = $this->fsql->query("TRUNCATE TABLE myTable");
        $this->assertFalse($result);
        $this->assertEquals("Table db1.public.myTable does not exist", trim($this->fsql->error()));
    }

    public function testTruncateSuccess()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("TRUNCATE TABLE customers");
        $this->assertTrue($result);

        $entries = $table->getEntries();
        $this->assertTrue(count($entries) === 0);
    }

    public function testTruncateContinueIdentity()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns2);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("TRUNCATE TABLE customers CONTINUE IDENTITY");
        $this->assertTrue($result);

        $entries = $table->getEntries();
        $this->assertTrue(count($entries) === 0);

        $identity = $table->getIdentity();
        $this->assertTrue($identity !== null);
        $this->assertEquals($identity->current, 3);
    }

    public function testTruncateRestartIdentity()
    {
        $customers = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns2);
        $cursor = $customers->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $customers->commit();

        $result = $this->fsql->query("TRUNCATE TABLE customers RESTART IDENTITY");
        $this->assertTrue($result);

        $table = $this->fsql->current_schema()->getTable('customers');
        $entries = $table->getEntries();
        $this->assertTrue(count($entries) === 0);

        $identity = $table->getIdentity();
        $this->assertTrue($identity !== null);
        $this->assertEquals($identity->current, 1);
    }


}
