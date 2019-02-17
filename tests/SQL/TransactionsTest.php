K<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class TransactionsTest extends BaseTest
{
    private $fsql;

    private static $columns = [
        'personId' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $entries = [
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

    public function testBeginRollback()
    {
        $this->assertTrue($this->fsql->is_auto_commit());

        $result = $this->fsql->query("BEGIN WORK;");
        $this->assertTrue($result);
        $this->assertFalse($this->fsql->is_auto_commit());

        $table = CachedTable::create($this->fsql->current_schema(), "students", self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $transaction = $this->fsql->get_transaction();
        $transaction->markTableAsUpdated($table);

        $this->assertEquals(self::$entries, $table->getEntries());

        $result = $this->fsql->query("ROLLBACK WORK;");
        $this->assertTrue($result);
        $this->assertTrue($this->fsql->is_auto_commit());

        $this->assertSame([], $table->getEntries());
    }

    public function testBeginCommit()
    {
        $this->assertTrue($this->fsql->is_auto_commit());

        $result = $this->fsql->query("BEGIN");
        $this->assertTrue($result);
        $this->assertFalse($this->fsql->is_auto_commit());

        $table = CachedTable::create($this->fsql->current_schema(), "students", self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $transaction = $this->fsql->get_transaction();
        $transaction->markTableAsUpdated($table);

        $result = $this->fsql->query("COMMIT WORK;");
        $this->assertTrue($result);
        $this->assertTrue($this->fsql->is_auto_commit());

        $this->assertEquals(self::$entries, $table->getEntries());
    }

    public function testStartRollback()
    {
        $this->assertTrue($this->fsql->is_auto_commit());

        $result = $this->fsql->query("START TRANSACTION;");
        $this->assertTrue($result);
        $this->assertFalse($this->fsql->is_auto_commit());

        $table = CachedTable::create($this->fsql->current_schema(), "students", self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $transaction = $this->fsql->get_transaction();
        $transaction->markTableAsUpdated($table);

        $this->assertEquals(self::$entries, $table->getEntries());

        $result = $this->fsql->query("ROLLBACK WORK;");
        $this->assertTrue($result);
        $this->assertTrue($this->fsql->is_auto_commit());

        $this->assertSame([], $table->getEntries());
    }

    public function testStartCommit()
    {
        $this->assertTrue($this->fsql->is_auto_commit());

        $result = $this->fsql->query("START TRANSACTION");
        $this->assertTrue($result);
        $this->assertFalse($this->fsql->is_auto_commit());

        $table = CachedTable::create($this->fsql->current_schema(), "students", self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $transaction = $this->fsql->get_transaction();
        $transaction->markTableAsUpdated($table);

        $result = $this->fsql->query("COMMIT WORK;");
        $this->assertTrue($result);
        $this->assertTrue($this->fsql->is_auto_commit());

        $this->assertEquals(self::$entries, $table->getEntries());
    }
}
