<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class SelectTest extends fSQLBaseTest
{
    private $fsql;

    static $columns1 = array(
        'personId' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 1, 'restraint' => array()),
        'firstName' => array ('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => array()),
        'lastName' => array ('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => array()),
        'city' => array('type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => array())
    );

    static $entries1 = array(
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
        array(11, 'homer', null, 'boston'),
        array(12, null, 'king', 'tokyo'),
    );

    static $columns2 = array(
        'id' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'person' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'item' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'quantity' => array('type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'orderDate' => array('type' => 'd', 'auto' => 0, 'default' => 0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
        'total' => array('type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => array()),
    );

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
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

        while($total_elements > 1) {
            if($array[$i][$key] === null && $array[$i+1][$key] !== null && !$nulls_first) {
                return false;
            } else if($array[$i][$key] !== null && $array[$i+1][$key] === null && $nulls_first) {
                return false;
            } else if($ascending && $array[$i][$key] > $array[$i+1][$key]) {
                return false;
            } else if(!$ascending && $array[$i][$key] < $array[$i+1][$key]) {
                return false;
            } else if($array[$i][$key] == $array[$i+1][$key]) {
                $new_option = next($options);
                while($new_option !== false)
                {
                    list($new_key, $new_ascending, $new_nulls_first) = $new_option;
                    if($array[$i][$new_key] === null || $array[$i+1][$new_key] === null) {
                        if($array[$i+1][$new_key] !== null && !$new_nulls_first) {
                            return false;
                        } else if($array[$i+1][$new_key] === null && $new_nulls_first) {
                            return false;
                        }
                    } else if($new_ascending && $array[$i][$new_key] > $array[$i+1][$new_key]) {
                        return false;
                    } else if(!$new_ascending && $array[$i][$new_key] < $array[$i+1][$new_key]) {
                        return false;
                    }
                    $new_option = next($options);
                }

                reset($options);
                while(key($options) !== $current_key)
                    next($options);
            }

            $i++;
            $total_elements--;
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

    public function testSelectAll()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(self::$entries1, $results);
    }

    public function testOrderByColumnIndex()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers ORDER BY 3, 2");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $tosort = array( array( 2, true, true ), array( 1, true, true) );
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOrderByColumnIndexBad()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers ORDER BY 5");
        $this->assertFalse($result);
        $this->assertEquals('ORDER BY: Invalid column number: 5', trim($this->fsql->error()));
    }

    public function testOrderByColumnName()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers ORDER BY lastName, firstName");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $tosort = array( array( 2, true, true ), array( 1, true, true) );
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOrderByColumnNameBad()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers ORDER BY garbage");
        $this->assertFalse($result);
        $this->assertEquals('Unknown column/alias in ORDER BY clause: garbage', trim($this->fsql->error()));
    }

    public function testOrderByDescAsc()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers ORDER BY lastName ASC, firstName DESC");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $tosort = array( array( 2, true, true ), array( 1, false, true) );
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOrderByNullsFirstLast()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers ORDER BY lastName NULLS FIRST, firstName NULLS LAST");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $tosort = array( array( 2, true, true ), array( 1, true, false) );
        $this->assertTrue($this->isArrayKeySorted($results, $tosort));
    }

    public function testOffsetOnly()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers OFFSET 5 ROWS");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);

        $this->assertEquals(array_slice(self::$entries1, 5), $results);
    }

    public function testOffsetFetchFirstNoLength()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers OFFSET 5 ROWS FETCH FIRST ROW ONLY");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(array_slice(self::$entries1, 5, 1), $results);
    }

    public function testOffsetFetchFirstLength()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers OFFSET 5 ROWS FETCH FIRST 3 ROWS ONLY");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(array_slice(self::$entries1, 5, 3), $results);
    }

    public function testFetchFirstLength()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers FETCH NEXT 3 ROWS ONLY");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(array_slice(self::$entries1, 0, 3), $results);
    }

    public function testOffsetAndLimit()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers OFFSET 3 ROWS LIMIT 5");
        $this->assertFalse($result);
        $this->assertEquals('LIMIT forbidden when FETCH FIRST or OFFSET already specified', trim($this->fsql->error()));
    }

    public function testFetchFirstAndLimit()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers FETCH NEXT 3 ROWS ONLY LIMIT 5");
        $this->assertFalse($result);
        $this->assertEquals('LIMIT forbidden when FETCH FIRST or OFFSET already specified', trim($this->fsql->error()));
    }

    public function testLimitLengthOnly()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers LIMIT 5");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(array_slice(self::$entries1, 0, 5), $results);
    }

    public function testLimitThenOffset()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers LIMIT 4 OFFSET 3");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(array_slice(self::$entries1, 3, 4), $results);
    }

    public function testLimitCommas()
    {
        $table = fSQLCachedTable::create($this->fsql->current_schema(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers LIMIT 3, 4");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(array_slice(self::$entries1, 3, 4), $results);
    }
}
