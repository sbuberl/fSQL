<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\ResultSet;

class ResultSetTest extends BaseTest
{
    private static $columns = array('id', 'firstName', 'lastName', 'city');

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

    public function testFetchAllEmpty()
    {
        $empty = array();
        $results = new ResultSet(array('myColumn'), $empty);

        $this->assertEquals($empty, $results->fetchAll(FSQL_NUM));
        $this->assertEquals($empty, $results->fetchAll(FSQL_ASSOC));
        $this->assertEquals($empty, $results->fetchAll(FSQL_BOTH));
    }

    public function testFetchAll()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $this->assertEquals(self::$entries, $results->fetchAll(FSQL_NUM));

        $assocResult = $results->fetchAll(FSQL_ASSOC);
        foreach ($assocResult as $entry) {
            $this->assertEquals(array_combine(self::$columns, $entry), $entry);
        }

        $bothResult = $results->fetchAll(FSQL_BOTH);
        foreach ($assocResult as $entry) {
            $this->assertEquals(array_merge($entry, array_combine(self::$columns, $entry)), $entry);
        }
    }

    public function testFetchAssoc()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetchAssoc()) !== false) {
            $this->assertEquals(array_combine(self::$columns, self::$entries[$i++]), $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testFetchRow()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetchRow()) !== false) {
            $this->assertEquals(self::$entries[$i++], $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testFetchBoth()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetchBoth()) !== false) {
            $entry = self::$entries[$i++];
            $this->assertEquals(array_merge($entry, array_combine(self::$columns, $entry)), $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testFetchSingleEmpty()
    {
        $results = new ResultSet(self::$columns, array());
        $this->assertFalse($results->fetchSingle(5));
    }

    public function testFetchSingleIndex()
    {
        $results = new ResultSet(self::$columns, self::$entries);
        $this->assertEquals('smith', $results->fetchSingle(2));
    }

    public function testFetchSingleName()
    {
        $results = new ResultSet(self::$columns, self::$entries);
        $this->assertEquals('chicago', $results->fetchSingle('city'));
    }

    public function testFetchObject()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetchObject()) !== false) {
            $this->assertEquals((object) array_combine(self::$columns, self::$entries[$i++]), $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testDataSeekBad()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $this->assertFalse($results->dataSeek(-1));
        $this->assertFalse($results->dataSeek(count(self::$entries)));
    }

    public function testDataSeek()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $success = $results->dataSeek(7);
        $this->assertTrue($success !== false);

        $row = $results->fetchRow();
        $this->assertEquals(self::$entries[7], $row);
    }

    public function testNumRows()
    {
        $emptySet = new ResultSet(self::$columns, array());
        $this->assertEquals(0, $emptySet->numRows());

        $set = new ResultSet(self::$columns, self::$entries);
        $this->assertEquals(count(self::$entries), $set->numRows());
    }

    public function testNumFields()
    {
        $set = new ResultSet(self::$columns, self::$entries);
        $this->assertEquals(count(self::$columns), $set->numFields());
    }

    public function testFetchField()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($field = $results->fetchField()) !== false) {
            $this->assertTrue(is_object($field));
            $this->assertEquals(self::$columns[$i++], $field->name);
        }
        $this->assertEquals(count(self::$columns), $i);
    }

    public function testFieldSeekBad()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $this->assertFalse($results->fieldSeek(-1));
        $this->assertFalse($results->fieldSeek(count(self::$columns)));
    }

    public function testFieldSeek()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $success = $results->fieldSeek(2);
        $this->assertTrue($success !== false);

        $field = $results->fetchField();
        $this->assertEquals(self::$columns[2], $field->name);
    }
}
