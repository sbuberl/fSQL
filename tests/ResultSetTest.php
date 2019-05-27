<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\ResultSet;

class ResultSetTest extends BaseTest
{
    private static $columns = ['id', 'firstName', 'lastName', 'city'];

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
    ];

    public function testFree()
    {
        $results = new ResultSet(self::$columns, self::$entries);
        $results->free();
    }

    public function testFetchAllEmpty()
    {
        $empty = [];
        $results = new ResultSet(array('myColumn'), $empty);

        $this->assertEquals($empty, $results->fetch_all(ResultSet::FETCH_NUM));
        $this->assertEquals($empty, $results->fetch_all(ResultSet::FETCH_ASSOC));
        $this->assertEquals($empty, $results->fetch_all(ResultSet::FETCH_BOTH));
    }

    public function testFetchAll()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $this->assertEquals(self::$entries, $results->fetch_all(ResultSet::FETCH_NUM));

        $assocResult = $results->fetch_all(ResultSet::FETCH_ASSOC);
        foreach ($assocResult as $entry) {
            $this->assertEquals(array_combine(self::$columns, $entry), $entry);
        }

        $bothResult = $results->fetch_all(ResultSet::FETCH_BOTH);
        foreach ($assocResult as $entry) {
            $this->assertEquals(array_merge($entry, array_combine(self::$columns, $entry)), $entry);
        }
    }

    public function testFetchAssoc()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetch_assoc()) !== null) {
            $this->assertEquals(array_combine(self::$columns, self::$entries[$i++]), $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testFetchRow()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetch_row()) !== null) {
            $this->assertEquals(self::$entries[$i++], $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testFetchBoth()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($row = $results->fetch_both()) !== null) {
            $entry = self::$entries[$i++];
            $this->assertEquals(array_merge($entry, array_combine(self::$columns, $entry)), $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testFetchSingleEmpty()
    {
        $results = new ResultSet(self::$columns, array());
        $this->assertnull($results->fetchSingle(5));
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
        while (($row = $results->fetch_object()) !== null) {
            $this->assertEquals((object) array_combine(self::$columns, self::$entries[$i++]), $row);
        }
        $this->assertEquals(count(self::$entries), $i);
    }

    public function testDataSeekBad()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $this->assertFalse($results->data_seek(-1));
        $this->assertFalse($results->data_seek(count(self::$entries)));
    }

    public function testDataSeek()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $success = $results->data_seek(7);
        $this->assertTrue($success !== null);

        $row = $results->fetch_row();
        $this->assertEquals(self::$entries[7], $row);
    }

    public function testNumRows()
    {
        $emptySet = new ResultSet(self::$columns, array());
        $this->assertEquals(0, $emptySet->num_rows());

        $set = new ResultSet(self::$columns, self::$entries);
        $this->assertEquals(count(self::$entries), $set->num_rows());
    }

    public function testFieldCount()
    {
        $set = new ResultSet(self::$columns, self::$entries);
        $this->assertEquals(count(self::$columns), $set->field_count());
    }

    public function testFetchField()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 0;
        while (($field = $results->fetch_field()) !== false) {
            $this->assertTrue(is_object($field));
            $this->assertEquals(self::$columns[$i++], $field->name);
        }
        $this->assertEquals(count(self::$columns), $i);
    }

    public function testFetchFieldDirect()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $field = $results->fetch_field_direct(2);
        $this->assertTrue(is_object($field));
        $this->assertEquals(self::$columns[2], $field->name);
    }

    public function testFetchFields()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $fields = $results->fetch_fields();

        $expected = [];
        while (($field = $results->fetch_field()) !== false) {
            $expected[] = $field;
        }
        $this->assertEquals($expected, $fields);
    }

    public function testCurrentField()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $i = 1;
        while (($field = $results->fetch_field()) !== false) {
            $this->assertEquals($i++, $results->current_field());
        }
    }

    public function testFieldSeekBad()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $this->assertFalse($results->field_seek(-1));
        $this->assertFalse($results->field_seek(count(self::$columns)));
    }

    public function testFieldSeek()
    {
        $results = new ResultSet(self::$columns, self::$entries);

        $success = $results->field_seek(2);
        $this->assertTrue($success !== false);

        $field = $results->fetch_field();
        $this->assertEquals(self::$columns[2], $field->name);
    }
}
