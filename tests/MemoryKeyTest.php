<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\Key;
use FSQL\Database\MemoryKey;
use FSQL\Environment;

class MemoryKeyTest extends BaseTest
{
    private static $columns1 = [
        'personId' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'p', 'null' => 1, 'restraint' => []],
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

    public function testConstructor()
    {
        $keyName = 'garbage';
        $keyType = Key::PRIMARY;
        $keyColumns = [0];
        $key = new MemoryKey($keyName, $keyType, $keyColumns);
        $this->assertEquals($keyName, $key->name());
        $this->assertEquals($keyType, $key->type());
        $this->assertEquals($keyColumns, $key->columns());
        $this->assertEquals(0, $key->count());
    }

    public function testExtractIndexOneKey()
    {
        $keyColumns = [0];
        $key = new MemoryKey('my_primary', Key::PRIMARY, [0]);
        $index = $key->extractIndex(self::$entries1[1]);
        $this->assertEquals($index, self::$entries1[1][0]);
    }

    public function testExtractIndexTwoKeys()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index = $key->extractIndex(self::$entries1[1]);
        $this->assertEquals($index, [self::$entries1[1][1], self::$entries1[1][2]]);
    }

    public function testExtractIndexThreeKeys()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2, 3]);
        $index = $key->extractIndex(self::$entries1[1]);
        $this->assertEquals($index, [self::$entries1[1][1], self::$entries1[1][2], self::$entries1[1][3]]);
    }

    public function testExtractIndexFourKeys()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [0, 1, 2, 3]);
        $index = $key->extractIndex(self::$entries1[1]);
        $this->assertEquals($index, [self::$entries1[1][0], self::$entries1[1][1], self::$entries1[1][2], self::$entries1[1][3]]);
    }

    public function testAddEntry()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index = $key->extractIndex(self::$entries1[1]);
        $key->addEntry(1, $index);
        $this->assertEquals(1, $key->count());
    }

    public function testAddEntries()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index1 = $key->extractIndex(self::$entries1[1]);
        $key->addEntry(1, $index1);
        $index2 = $key->extractIndex(self::$entries1[5]);
        $key->addEntry(5, $index2);
        $index3 = $key->extractIndex(self::$entries1[8]);
        $key->addEntry(8, $index3);
        $this->assertEquals(3, $key->count());
    }

    public function testLookupEmpty()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index1 = $key->extractIndex(self::$entries1[1]);
        $rowId = $key->lookup($index1);
        $this->assertSame(false, $rowId);
    }
    public function testLookupSingleKey()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [0]);
        $index1 = $key->extractIndex(self::$entries1[1]);
        $key->addEntry(1, $index1);
        $index2 = $key->extractIndex(self::$entries1[5]);
        $key->addEntry(5, $index2);
        $index3 = $key->extractIndex(self::$entries1[8]);
        $key->addEntry(8, $index3);
        $rowId = $key->lookup($index2);
        $this->assertSame(5, $rowId);
    }

    public function testLookupMultipleKey()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index1 = $key->extractIndex(self::$entries1[1]);
        $key->addEntry(1, $index1);
        $index2 = $key->extractIndex(self::$entries1[5]);
        $key->addEntry(5, $index2);
        $index3 = $key->extractIndex(self::$entries1[8]);
        $key->addEntry(8, $index3);
        $rowId = $key->lookup($index2);
        $this->assertSame(5, $rowId);
    }

    public function testUpdateEntry()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index1 = $key->extractIndex(self::$entries1[1]);
        $key->addEntry(1, $index1);
        $index2 = $key->extractIndex(self::$entries1[5]);
        $key->addEntry(5, $index2);
        $index3 = $key->extractIndex(self::$entries1[8]);
        $key->addEntry(8, $index3);
        $this->assertEquals(3, $key->count());

        $key->updateEntry(5, $index2);

        $rowId = $key->lookup($index2);
        $this->assertSame(5, $rowId);
        $this->assertEquals(3, $key->count());
    }

    public function testDeleteEntry()
    {
        $key = new MemoryKey('my_primary', Key::PRIMARY, [1, 2]);
        $index1 = $key->extractIndex(self::$entries1[1]);
        $key->addEntry(1, $index1);
        $index2 = $key->extractIndex(self::$entries1[5]);
        $key->addEntry(5, $index2);
        $index3 = $key->extractIndex(self::$entries1[8]);
        $key->addEntry(8, $index3);
        $this->assertEquals(3, $key->count());

        $key->deleteEntry(5);

        $rowId = $key->lookup($index2);
        $this->assertSame(false, $rowId);
        $this->assertEquals(2, $key->count());
    }
}
