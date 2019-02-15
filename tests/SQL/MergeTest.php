<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class MergeTest extends BaseTest
{
    private $fsql;

    private static $columns = [
        'id' => ['type' => 'i', 'auto' => 0, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => []],
        'name' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 0, 'restraint' => []],
        'price' => ['type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 0, 'restraint' => []],
    ];

    private static $productEntries = [
        [101, 'TEA', 10.00],
        [102, 'COFFEE', 15.00],
        [103, 'BISCUIT', 20.00],
    ];

    private static $updatedEntries = [
        [101, 'TEA', 10.00],
        [102, 'COFFEE', 25.00],
        [104, 'CHIPS', 22.00],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testMergeInsertOnly()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query('MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN NOT MATCHED THEN
                INSERT (id,name,price) VALUES (SOURCE.id,SOURCE.name,SOURCE.price);');
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [102, 'COFFEE', 15.00],
            [103, 'BISCUIT', 20.00],
            [104, 'CHIPS', 22.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testMergeInsertOnlyAnd()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query("MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN NOT MATCHED AND SOURCE.name != 'CHIPS' THEN
                INSERT (id,name,price) VALUES (SOURCE.id,SOURCE.name,SOURCE.price);");
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [102, 'COFFEE', 15.00],
            [103, 'BISCUIT', 20.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testMergeUpdateOnly()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query('MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN MATCHED THEN
                UPDATE SET TARGET.name = SOURCE.name, TARGET.price = SOURCE.price;');
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [102, 'COFFEE', 25.00],
            [103, 'BISCUIT', 20.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testMergeUpdateOnlyAnd()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query("MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN MATCHED AND SOURCE.name != 'COFFEE' THEN
                UPDATE SET TARGET.name = SOURCE.name, TARGET.price = SOURCE.price;");
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [102, 'COFFEE', 15.00],
            [103, 'BISCUIT', 20.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }


    public function testMergeDeleteOnly()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query('MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN MATCHED THEN DELETE');
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [103, 'BISCUIT', 20.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testMergeDeleteOnlyAnd()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query('MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN MATCHED AND SOURCE.id != 101 THEN DELETE');
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [103, 'BISCUIT', 20.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testMergeUpdateAndInsert()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query('MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN MATCHED THEN
                UPDATE SET TARGET.name = SOURCE.name, TARGET.price = SOURCE.price
            WHEN NOT MATCHED THEN
                INSERT (id,name,price) VALUES (SOURCE.id,SOURCE.name,SOURCE.price);');
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [102, 'COFFEE', 25.00],
            [103, 'BISCUIT', 20.00],
            [104, 'CHIPS', 22.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }

    public function testMergeUpdateAndInsertAndDelete()
    {
        $products = CachedTable::create($this->fsql->current_schema(), 'products', self::$columns);
        $cursor = $products->getWriteCursor();
        foreach (self::$productEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $products->commit();

        $updated = CachedTable::create($this->fsql->current_schema(), 'updated', self::$columns);
        $cursor = $updated->getWriteCursor();
        foreach (self::$updatedEntries as $entry) {
            $cursor->appendRow($entry);
        }
        $updated->commit();

        $result = $this->fsql->query('MERGE INTO products AS TARGET
            USING updated AS SOURCE
            ON (TARGET.id = SOURCE.id)
            WHEN MATCHED AND SOURCE.id != 102 THEN
                UPDATE SET TARGET.name = SOURCE.name, TARGET.price = SOURCE.price
            WHEN MATCHED AND SOURCE.id = 102 THEN
                DELETE
            WHEN NOT MATCHED THEN
                INSERT (id,name,price) VALUES (SOURCE.id,SOURCE.name,SOURCE.price);');
        $this->assertTrue($result !== false);

        $result = $this->fsql->query('SELECT * FROM products');
        $this->assertTrue($result !== false);

        $expected = [
            [101, 'TEA', 10.00],
            [103, 'BISCUIT', 20.00],
            [104, 'CHIPS', 22.00],
        ];

        $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
        $this->assertEquals($expected, $results);
    }
}
