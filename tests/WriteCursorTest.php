<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Database\WriteCursor;
use FSQL\Environment;

class WriteCursorTest extends BaseTest
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

    public function testAppendRowEmpty()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();

        $this->assertEquals(0, $writeCursor->key());

        $rowId = $writeCursor->appendRow(self::$entries[0]);
        $tableEntries = $table->getEntries();

        $this->assertEquals(0, $writeCursor->key());
        $this->assertEquals(0, $rowId);
        $this->assertEquals($tableEntries[0], self::$entries[0]);
    }

    public function testAppendRowEmptyId()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();

        $this->assertEquals(0, $writeCursor->key());

        $rowId = $writeCursor->appendRow(self::$entries[0], 4);
        $tableEntries = $table->getEntries();

        $this->assertEquals(4, $rowId);
        $this->assertEquals($tableEntries[4], self::$entries[0]);
    }

    public function testAppendRow()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $rowId = $writeCursor->appendRow($entry);
            $this->assertTrue($rowId !== false);
        }

        $tableEntries = $table->getEntries();
        $this->assertEquals($tableEntries, self::$entries);
    }

    public function testAppendRowId()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $rowId = $writeCursor->appendRow($entry);
            $this->assertTrue($rowId !== false);
        }

        $rowId = $writeCursor->appendRow(self::$entries[0], 13);
        $tableEntries = $table->getEntries();

        $expected = [
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
            13 =>  [1, 'bill', 'smith', 'chicago'],
        ];

        $this->assertEquals(13, $rowId);
        $this->assertEquals($tableEntries, $expected);
    }
}
