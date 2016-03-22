<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\TableCursor;

class TableCursorTest extends BaseTest
{
    private static $entries = array(
        array(1, 'bill', 'smith', 'chicago'),
        array(2, 'jon', 'doe', 'baltimore'),
        array(3, 'mary', 'shelley', 'seattle'),
        array(4, 'stephen', 'king', 'derry'),
        array(5, 'bart', 'simpson', 'springfield'),
        array(6, 'jane', 'doe', 'seattle'),
        array(7, 'bram', 'stoker', 'new york'),
        array(8, 'douglas', 'adams', 'london'),
        array(9, 'bill', 'johnson', 'derry'),
        array(10, 'jon', 'doe', 'new york'),
    );

    public function testConstructorEmpty()
    {
        $empty = array();
        $tableCursor = new TableCursor($empty);
        $this->assertFalse($tableCursor->valid());
        $this->assertFalse($tableCursor->key());
        $this->assertFalse($tableCursor->current());
        $this->assertEquals(0, $tableCursor->count());
    }

    public function testConstructor()
    {
        $tableCursor = new TableCursor(self::$entries);
        $this->assertTrue($tableCursor->valid());
        $this->assertEquals(0, $tableCursor->key());
        $this->assertEquals(self::$entries[0], $tableCursor->current());
        $this->assertEquals(count(self::$entries), $tableCursor->count());
    }

    public function testNextLoop()
    {
        $tableCursor = new TableCursor(self::$entries);

        $count = count(self::$entries);
        for ($i = 0; $i < $count; ++$i) {
            $this->assertTrue($tableCursor->valid());
            $this->assertEquals($i, $tableCursor->key());
            $this->assertEquals(self::$entries[$i], $tableCursor->current());
            $tableCursor->next();
        }
        $this->assertFalse($tableCursor->valid());
    }

    public function testForeach()
    {
        $tableCursor = new TableCursor(self::$entries);

        foreach ($tableCursor as $i => $value) {
            $this->assertTrue($tableCursor->valid());
            $this->assertEquals($i, $tableCursor->key());
            $this->assertEquals(self::$entries[$i], $tableCursor->current());
            $tableCursor->next();
        }
        $this->assertFalse($tableCursor->valid());
    }

    public function testNextRewind()
    {
        $tableCursor = new TableCursor(self::$entries);

        $this->assertEquals(0, $tableCursor->key());
        $this->assertEquals(self::$entries[0], $tableCursor->current());

        $tableCursor->next();
        $tableCursor->next();
        $tableCursor->next();
        $tableCursor->next();
        $tableCursor->next();

        $this->assertEquals(5, $tableCursor->key());
        $this->assertEquals(self::$entries[5], $tableCursor->current());

        $tableCursor->rewind();

        $this->assertEquals(0, $tableCursor->key());
        $this->assertEquals(self::$entries[0], $tableCursor->current());
    }

    public function testSeekInvalidIndex()
    {
        $tableCursor = new TableCursor(self::$entries);

        $this->assertEquals(0, $tableCursor->key());
        $this->assertEquals(self::$entries[0], $tableCursor->current());

        $tableCursor->seek(13);

        $this->assertEquals(0, $tableCursor->key());
        $this->assertEquals(self::$entries[0], $tableCursor->current());
    }

    public function testSeek()
    {
        $tableCursor = new TableCursor(self::$entries);

        $this->assertEquals(0, $tableCursor->key());
        $this->assertEquals(self::$entries[0], $tableCursor->current());

        $tableCursor->seek(8);

        $this->assertEquals(8, $tableCursor->key());
        $this->assertEquals(self::$entries[8], $tableCursor->current());
    }
}
