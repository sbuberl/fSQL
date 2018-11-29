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
        $this->assertTrue($writeCursor->isUncommitted());
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
        $this->assertTrue($writeCursor->isUncommitted());
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
        $this->assertTrue($writeCursor->isUncommitted());
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
        $this->assertTrue($writeCursor->isUncommitted());
    }

    public function testGetNewRowsEmpty()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        $newRows = $writeCursor->getNewRows();

        $expected = [
            [1, 'bill', 'smith', 'chicago'],
            [5, 'bart', 'simpson', 'springfield'],
            [8, 'douglas', 'adams', 'london'],
        ];

        $this->assertEquals([], $newRows);
    }

    public function testGetNewRows()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        $rowId = $writeCursor->appendRow(self::$entries[0]);
        $rowId = $writeCursor->appendRow(self::$entries[4]);
        $rowId = $writeCursor->appendRow(self::$entries[7]);
        $newRows = $writeCursor->getNewRows();

        $expected = [
            [1, 'bill', 'smith', 'chicago'],
            [5, 'bart', 'simpson', 'springfield'],
            [8, 'douglas', 'adams', 'london'],
        ];

        $this->assertEquals($newRows, $expected);
    }

    public function testUpdateRowEmpty()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();

        $updates = [3 => 'berlin', 1 => 'dale'];
        $writeCursor->updateRow($updates);

        $tableEntries = $table->getEntries();
        $this->assertEquals([], $tableEntries);
        $this->assertFalse($writeCursor->isUncommitted());
    }

    public function testUpdateRow()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $rowId = $writeCursor->appendRow($entry);
            $this->assertTrue($rowId !== false);
        }
        $table->commit();

        $updates = [3 => 'berlin', 1 => 'dale'];
        foreach($writeCursor as $customer) {
            $writeCursor->updateRow($updates);
        }

        $expected = [
            [1, 'dale', 'smith', 'berlin'],
            [2, 'dale', 'doe', 'berlin'],
            [3, 'dale', 'shelley', 'berlin'],
            [4, 'dale', 'king', 'berlin'],
            [5, 'dale', 'simpson', 'berlin'],
            [6, 'dale', 'doe', 'berlin'],
            [7, 'dale', 'stoker', 'berlin'],
            [8, 'dale', 'adams', 'berlin'],
            [9, 'dale', 'johnson', 'berlin'],
            [10, 'dale', 'doe', 'berlin'],
            [11, 'dale', null, 'berlin'],
            [12, 'dale', 'king', 'berlin'],
        ];

        $tableEntries = $table->getEntries();
        $this->assertEquals($tableEntries, $expected);
        $this->assertTrue($writeCursor->isUncommitted());
    }

    public function testDeleteRowEmpty()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();

        $result = $writeCursor->deleteRow();
        $this->assertFalse($result);

        $tableEntries = $table->getEntries();
        $this->assertEquals([], $tableEntries);
        $this->assertFalse($writeCursor->isUncommitted());
    }

    public function testDeleteRow()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $rowId = $writeCursor->appendRow($entry);
            $this->assertTrue($rowId !== false);
        }
        $table->commit();

        $writeCursor->deleteRow();
        $writeCursor->seek(5);
        $writeCursor->deleteRow();
        $writeCursor->deleteRow();

        $expected = [
            [2, 'jon', 'doe', 'baltimore'],
            [3, 'mary', 'shelley', 'seattle'],
            [4, 'stephen', 'king', 'derry'],
            [5, 'bart', 'simpson', 'springfield'],
            [6, 'jane', 'doe', 'seattle'],
            [9, 'bill', 'johnson', 'derry'],
            [10, 'jon', 'doe', 'new york'],
            [11, 'homer', null, 'boston'],
            [12, null, 'king', 'tokyo'],
        ];

        $tableEntries = $table->getEntries();
        $this->assertEquals($expected, array_values($tableEntries));
        $this->assertTrue($writeCursor->isUncommitted());
    }

    public function testDeleteRowAll()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $writeCursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $rowId = $writeCursor->appendRow($entry);
            $this->assertTrue($rowId !== false);
        }
        $table->commit();

        for($writeCursor->rewind(); $writeCursor->valid(); $writeCursor->deleteRow()) {
        }

        $tableEntries = $table->getEntries();
        $this->assertEquals([], $tableEntries);
        $this->assertTrue($writeCursor->isUncommitted());
    }
}
