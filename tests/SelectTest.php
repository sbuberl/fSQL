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
    );

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testSelectAll()
    {
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
        foreach(self::$entries1 as $entry) {
            $table->insertRow($entry);
        }
        $table->commit();

        $result = $this->fsql->query("SELECT * FROM customers");
        $this->assertTrue($result !== false);

        $results = $this->fsql->fetch_all($result, FSQL_NUM);
        $this->assertEquals(self::$entries1, $results);
    }

    public function testOffsetOnly()
    {
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
        $table = fSQLCachedTable::create($this->fsql->current_db(), 'customers', self::$columns1);
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
