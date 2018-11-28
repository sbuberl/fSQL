<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;

class DeleteTest extends BaseTest
{
    private $fsql;

    private static $columns1 = [
        'personId' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => []],
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

    public function testDeleteAll()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('DELETE FROM customers');
        $this->assertTrue($result !== false);

        $this->assertEquals([], $table->getEntries());
        $this->assertEquals(12, $this->fsql->affected_rows());
    }

    public function testDeleteWhere()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("DELETE FROM customers WHERE city = 'derry' OR firstName = 'jon'");
        $this->assertTrue($result !== false);

        $expected = [
            [1, 'bill', 'smith', 'chicago'],
            [3, 'mary', 'shelley', 'seattle'],
            [5, 'bart', 'simpson', 'springfield'],
            [6, 'jane', 'doe', 'seattle'],
            [7, 'bram', 'stoker', 'new york'],
            [8, 'douglas', 'adams', 'london'],
            [11, 'homer', null, 'boston'],
            [12, null, 'king', 'tokyo'],
        ];

        $this->assertEquals(array_values($expected), array_values($table->getEntries()));
        $this->assertEquals(4, $this->fsql->affected_rows());
    }
}
