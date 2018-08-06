<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class SelectTest extends BaseTest
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

    private static $columns2 = [
        'id' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'person' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'item' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'quantity' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'orderDate' => ['type' => 'd', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => []],
        'total' => ['type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    private function isArrayKeySorted($array, $options)
    {
        $i = 0;
        $total_elements = count($array);
        $current_option = current($options);
        $current_key = key($options);

        list($key, $ascending, $nulls_first) = $current_option;

        while ($total_elements > 1) {
            if ($array[$i][$key] === null && $array[$i + 1][$key] !== null && !$nulls_first) {
                return false;
            } elseif ($array[$i][$key] !== null && $array[$i + 1][$key] === null && $nulls_first) {
                return false;
            } elseif ($ascending && $array[$i][$key] > $array[$i + 1][$key]) {
                return false;
            } elseif (!$ascending && $array[$i][$key] < $array[$i + 1][$key]) {
                return false;
            } elseif ($array[$i][$key] == $array[$i + 1][$key]) {
                $new_option = next($options);
                while ($new_option !== false) {
                    list($new_key, $new_ascending, $new_nulls_first) = $new_option;
                    if ($array[$i][$new_key] === null || $array[$i + 1][$new_key] === null) {
                        if ($array[$i + 1][$new_key] !== null && !$new_nulls_first) {
                            return false;
                        } elseif ($array[$i + 1][$new_key] === null && $new_nulls_first) {
                            return false;
                        }
                    } elseif ($new_ascending && $array[$i][$new_key] > $array[$i + 1][$new_key]) {
                        return false;
                    } elseif (!$new_ascending && $array[$i][$new_key] < $array[$i + 1][$new_key]) {
                        return false;
                    }
                    $new_option = next($options);
                }

                reset($options);
                while (key($options) !== $current_key) {
                    next($options);
                }
            }

            ++$i;
            --$total_elements;
        }

        return true;
    }

    public function testSelectNoTable()
    {
        $result = $this->fsql->query("SELECT NULL, -3, 3.14, 'my string', CONCAT('x ', 'y ', 'z')");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals(array(null, -3, 3.14, 'my string', 'x y z'), $result);
    }

    public function testSelectCast()
    {
        $result = $this->fsql->query("SELECT CAST(NULL as INT), cast(-3 as TEXT), cast(3.14 AS INT), CAST('2.5' AS DOUBLE)");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals([null, '-3', 3, 2.5], $result);
    }

    public function testSelectExtract()
    {
        $result = $this->fsql->query("SELECT EXTRACT(YEAR FROM null), EXTRACT(YEAR FROM '2013-07-02'), EXTRACT(SECOND FROM '2013-07-02'), EXTRACT(HOUR FROM '2013-07-02 01:02:03'), EXTRACT(timezone_hour FROM '2013-07-02 01:02:03-05:00')");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals([null, 2013, 0, 1, -5], $result);
    }

    public function testSelectOverlay()
    {
        $result = $this->fsql->query("SELECT OVERLAY('Dxxxlas' placing NULL from 3 for 4), OVERLAY('Dxxxlas' placing 'oug' from 2), OVERLAY('Dxxxlas' placing 'oug' from 2 for 3)");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals([NULL, 'Doug', 'Douglas'], $result);
    }

    public function testSelectPosition()
    {
        $result = $this->fsql->query("SELECT POSITION(null in 'blah'), POSITION('q' in 'Donald Duck'), POSITION('Duck' in 'Donald Duck')");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals([NULL, 0, 8], $result);
    }

    public function testSelectSubstring()
    {
        $result = $this->fsql->query("SELECT SUBSTRING(null from 3 for 4), SUBSTRING('Donald Duck' from 8), SUBSTRING('Donald Duck' from 1 FOR 6)");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals([NULL, 'Duck', 'Donald'], $result);
    }

    public function testSelectTrim()
    {
        $result = $this->fsql->query("SELECT TRIM('x' from null), TRIM('x' from 'xxTomxxx'), TRIM(both 'x' FROM 'xxTomxxx'), TRIM(leading 'x' from 'xxTomxxx'), TRIM(trailing 'x' from 'xxTomxxx')");
        $this->assertTrue($result !== false);

        $result = $this->fsql->fetch_row($result);
        $this->assertEquals([null, 'Tom', 'Tom', 'Tomxxx', 'xxTom'], $result);
    }

    public function testSelectAll()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(self::$entries1, $results);
    }

    public function testSelectAllPrefix()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT c.* FROM customers as c');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(self::$entries1, $results);
    }

    public function testSelectColumn()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT firstName FROM customers");
        $this->assertTrue($result !== false);

        $expected = [
            ['bill'],
            ['jon'],
            ['mary'],
            ['stephen'],
            ['bart'],
            ['jane'],
            ['bram'],
            ['douglas'],
            ['bill'],
            ['jon'],
            ['homer'],
            [null],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testSelectSomeColumns()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT firstName, lastName FROM customers");
        $this->assertTrue($result !== false);

        $expected = [
            ['bill', 'smith'],
            ['jon', 'doe'],
            ['mary', 'shelley'],
            ['stephen', 'king'],
            ['bart', 'simpson'],
            ['jane', 'doe'],
            ['bram', 'stoker'],
            ['douglas', 'adams'],
            ['bill', 'johnson'],
            ['jon', 'doe'],
            ['homer', null],
            [null, 'king'],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testSelectLike()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT firstName, lastName, city FROM customers WHERE lastName LIKE '_o%'");
        $this->assertTrue($result !== false);

        $expected = [
            ['jon', 'doe', 'baltimore'],
            ['jane', 'doe', 'seattle'],
            ['bill', 'johnson', 'derry'],
            ['jon', 'doe', 'new york'],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testSelectNotLike()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT firstName, lastName, city FROM customers WHERE lastName NOT LIKE '_o%'");
        $this->assertTrue($result !== false);

        $expected = [
            ['bill', 'smith', 'chicago'],
            ['mary', 'shelley', 'seattle'],
            ['stephen', 'king', 'derry'],
            ['bart', 'simpson', 'springfield'],
            ['bram', 'stoker', 'new york'],
            ['douglas', 'adams', 'london'],
            [null, 'king', 'tokyo'],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testSelectRegexp()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT firstName, lastName, city FROM customers WHERE lastName REGEXP '.o.+'");
        $this->assertTrue($result !== false);

        $expected = [
            ['jon', 'doe', 'baltimore'],
            ['bart', 'simpson', 'springfield'],
            ['jane', 'doe', 'seattle'],
            ['bram', 'stoker', 'new york'],
            ['bill', 'johnson', 'derry'],
            ['jon', 'doe', 'new york'],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testSelectIn()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT firstName, lastName, city FROM customers WHERE city IN ('baltimore', 'derry', 'tampa', 'seattle')");
        $this->assertTrue($result !== false);

        $expected = [
            ['jon', 'doe', 'baltimore'],
            ['mary', 'shelley', 'seattle'],
            ['stephen', 'king', 'derry'],
            ['jane', 'doe', 'seattle'],
            ['bill', 'johnson', 'derry'],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testAggregateNoGroupBy()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT COUNT(lastName) FROM customers');
        $this->assertTrue($result !== false);

        $this->assertEquals(array(11), $this->fsql->fetch_row($result));
    }

    public function testGroupBy()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT city, COUNT(lastName) FROM customers GROUP BY city ORDER BY city');
        $this->assertTrue($result !== false);

        $expected = array(
            array('city' => 'baltimore', 'count' => 1),
            array('city' => 'boston', 'count' => 0),
            array('city' => 'chicago', 'count' => 1),
            array('city' => 'derry', 'count' => 2),
            array('city' => 'london', 'count' => 1),
            array('city' => 'new york', 'count' => 2),
            array('city' => 'seattle', 'count' => 2),
            array('city' => 'springfield', 'count' => 1),
            array('city' => 'tokyo', 'count' => 1),
        );

        $this->assertEquals($expected, $this->fsql->fetch_all($result));
    }

    public function testOrderByColumnIndex()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers ORDER BY 3, 2');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $tosort = array(array(2, true, true), array(1, true, true));
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOrderByColumnIndexBad()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers ORDER BY 5');
        $this->assertFalse($result);
        $this->assertEquals('ORDER BY: Invalid column number: 5', trim($this->fsql->error()));
    }

    public function testOrderByColumnName()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT firstName, lastName FROM customers ORDER BY lastName, firstName');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $tosort = array(array(1, true, true), array(0, true, true));
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOrderByColumnNameBad()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers ORDER BY garbage');
        $this->assertFalse($result);
        $this->assertEquals('Unknown column/alias in ORDER BY clause: garbage', trim($this->fsql->error()));
    }

    public function testOrderByDescAsc()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers ORDER BY lastName ASC, firstName DESC');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $tosort = array(array(2, true, true), array(1, false, true));
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOrderByNullsFirstLast()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers ORDER BY lastName NULLS FIRST, firstName NULLS LAST');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $tosort = array(array(2, true, true), array(1, true, false));
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOffsetOnly()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers OFFSET 5 ROWS');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);

        $this->assertEquals(array_slice(self::$entries1, 5), $results);
    }

    public function testOffsetFetchFirstNoLength()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers OFFSET 5 ROWS FETCH FIRST ROW ONLY');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(array_slice(self::$entries1, 5, 1), $results);
    }

    public function testOffsetFetchFirstLength()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers OFFSET 5 ROWS FETCH FIRST 3 ROWS ONLY');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(array_slice(self::$entries1, 5, 3), $results);
    }

    public function testFetchFirstLength()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers FETCH NEXT 3 ROWS ONLY');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(array_slice(self::$entries1, 0, 3), $results);
    }

    public function testOffsetAndLimit()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers OFFSET 3 ROWS LIMIT 5');
        $this->assertFalse($result);
        $this->assertEquals('LIMIT forbidden when FETCH FIRST or OFFSET already specified', trim($this->fsql->error()));
    }

    public function testFetchFirstAndLimit()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers FETCH NEXT 3 ROWS ONLY LIMIT 5');
        $this->assertFalse($result);
        $this->assertEquals('LIMIT forbidden when FETCH FIRST or OFFSET already specified', trim($this->fsql->error()));
    }

    public function testLimitLengthOnly()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers LIMIT 5');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(array_slice(self::$entries1, 0, 5), $results);
    }

    public function testLimitThenOffset()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers LIMIT 4 OFFSET 3');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(array_slice(self::$entries1, 3, 4), $results);
    }

    public function testLimitCommas()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries1 as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query('SELECT * FROM customers LIMIT 3, 4');
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals(array_slice(self::$entries1, 3, 4), $results);
    }
}
