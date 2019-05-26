<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\Statement;
use FSQL\ResultSet;

class StatementTest extends BaseTest
{
    private $fsql;

    private static $columns = [
        'personId' => ['type' => 'i', 'auto' => 1, 'default' => 0, 'key' => 'p', 'null' => 0, 'restraint' => [3, 0, 1, 1, 1, 10000, 0]],
        'firstName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'lastName' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'city' => ['type' => 's', 'auto' => 0, 'default' => '', 'key' => 'n', 'null' => 1, 'restraint' => []],
        'zip' => ['type' => 'f', 'auto' => 0, 'default' => 0.0, 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $entries = [
        [1, 'bill', 'smith', 'chicago', 12345],
        [2, 'jon', 'doe', 'baltimore', 54321],
        [3, 'mary', 'shelley', 'seattle', 98765],
        [4, 'stephen', 'king', 'derry', 42424],
        [5, 'bart', 'simpson', 'springfield', 55555],
        [6, 'jane', 'doe', 'seattle', 98765],
        [7, 'bram', 'stoker', 'new york', 56789],
        [8, 'douglas', 'adams', 'london', 99999],
        [9, 'bill', 'johnson', 'derry', 42424],
        [10, 'jon', 'doe', 'new york', 56789],
        [11, 'homer', null, 'boston', 22222],
        [12, null, 'king', 'tokyo', 11111],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $this->assertTrue($passed === true);
    }

    public function testBindParamNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $id = 5;
        $lastName = 'king';
        $zip = 99999;
        $passed = $statement->bind_param('isd', $id, $lastName, $zip);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a bind_param without a prepare");
    }

    public function testBindParamTypeParamMismatch()
    {
        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $lastName = 'king';
        $passed = $statement->bind_param('isd', $id, $lastName);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "bind_param's number of types in the string doesn't match number of parameters passed in");
    }

    public function testBindParamTCountMismatch()
    {
        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $lastName = 'king';
        $passed = $statement->bind_param('id', $id, $lastName);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "bind_param's number of params doesn't match number of params found in the query");
    }


    public function testBindParam()
    {
        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $lastName = 'king';
        $zip = 99999;
        $passed = $statement->bind_param('isd', $id, $lastName, $zip);
        $this->assertTrue($passed === true);
    }

    public function testExecuteNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->execute();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform an execute without a prepare");
    }

    public function testExecuteDMLError()
    {
        $statement = new Statement($this->fsql);
        $statement->prepare("INSERT INTO customers (personId, firstName, lastName, city) VALUES (1, 'John', 'Smith', 3.6, 'Los Angelos')");
        $passed = $statement->execute();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Table db1.public.customers does not exist");
    }

    public function testExecuteDML()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $statement = new Statement($this->fsql);
        $statement->prepare("INSERT INTO customers (personId, firstName, lastName, city, zip) VALUES (1, 'John', 'Smith', 'Los Angelos', 75677)");
        $passed = $statement->execute();
        $this->assertTrue($passed === true);
    }

    public function testExecuteNoParams()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = 5");
        $passed = $statement->execute();
        $this->assertTrue($passed === true);
    }

    public function testExecuteNoBindParam()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $passed = $statement->execute();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Found parameters the in query without a calling bind_param");
    }

    public function testExecuteParams()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $lastName = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $lastName, $zip);
        $passed = $statement->execute();
        $this->assertTrue($passed === true);
    }

    public function testMultipleExecuteParams()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $lastName = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $lastName, $zip);
        $passed = $statement->execute();
        $this->assertTrue($passed === true);
        $id = 1;
        $lastName = 'adams';
        $zip = null;
        $passed = $statement->execute();
        $this->assertTrue($passed === true);
    }

    public function testBindResultNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->bind_result($column1, $column2);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a bind_result without a prepare");
    }

    public function testBindResultNoResult()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $last = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $last, $zip);
        $passed = $statement->bind_result($firstName, $lastName, $city);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "No result set found for bind_result");
    }

    public function testBindResult()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $last = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $last, $zip);
        $statement->execute();
        $passed = $statement->bind_result($firstName, $lastName, $city);
        $this->assertTrue($passed === true);
    }

    public function testFetchNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->fetch();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a fetch without a prepare");
    }

    public function testFetchNoBindResult()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $last = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $last, $zip);
        $statement->execute();
        $passed = $statement->fetch();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a fetch without a bind_result");
    }

    public function testFetch()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $last = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $last, $zip);
        $statement->execute();
        $statement->bind_result($firstName, $lastName, $city);
        $i = 0;
        $expected = [
           ['stephen', 'king', 'derry'],
           ['bart', 'simpson', 'springfield'],
           ['douglas', 'adams', 'london'],
           [null, 'king', 'tokyo'],
        ];
        while($statement->fetch()) {
            $this->assertEquals($firstName, $expected[$i][0]);
            $this->assertEquals($lastName, $expected[$i][1]);
            $this->assertEquals($city, $expected[$i][2]);
            $i++;
        }
    }

    public function testStoreResultNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->store_result();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a store_result without a prepare");
    }

    public function testStoreResult()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $last = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $last, $zip);
        $statement->execute();
        $passed = $statement->store_result();
        $this->assertTrue($passed === true);
    }

    public function testGetResultNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->get_result();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a get_result without a prepare");
    }

    public function testGetResult()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = 5");
        $statement->execute();
        $result = $statement->get_result();
        $this->assertTrue($result instanceof ResultSet);
    }

    public function testResultMetadataNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->result_metadata();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a result_metadata without a prepare");
    }

    public function testResultMetadata()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = 5");
        $statement->execute();
        $result = $statement->result_metadata();
        $this->assertTrue($result instanceof ResultSet);
    }

    public function testFreeResult()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = 5");
        $statement->execute();
        $statement->free_result();
        $result = $statement->get_result();
        $this->assertTrue($result == null);
    }

    public function testDataSeekNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->data_seek(3);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a data_seek without a prepare");
    }

    public function testDataSeekNoStore()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $passed = $statement->data_seek(3);
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a data_seek without a store_result");
    }

    public function testDataSeek()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $statement->store_result();
        $passed = $statement->data_seek(3);
        $expected = ['stephen', 'king', 'derry'];
        $result = $statement->get_result();
        $this->assertTrue($passed === true);
        $this->assertEquals($expected, $result->fetch_row());
    }

    public function testClose()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $statement->store_result();
        $passed = $statement->close();
        $expected = ['stephen', 'king', 'derry'];
        $result = $statement->get_result();
        $this->assertTrue($passed === true);
    }

    public function testResetStored()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $statement->store_result();
        $passed = $statement->reset();
        $this->assertTrue($passed === true);
    }

    public function testResetNotStored()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $passed = $statement->reset();
        $this->assertTrue($passed === true);
    }


    public function testNumRowsNoPrepare()
    {
        $statement = new Statement($this->fsql);
        $passed = $statement->num_rows();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a num_rows without a prepare");
    }

    public function testNumRowsNoStore()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $passed = $statement->num_rows();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a num_rows without a store_result");
    }

    public function testNumRows()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $statement->execute();
        $statement->store_result();
        $result = $statement->num_rows();
        $this->assertEquals(12, $result);
    }

    public function testParamCountNoParams()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers");
        $result = $statement->param_count();
        $this->assertEquals(0, $result);
    }

    public function testParamCount()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $cursor = $table->getWriteCursor();
        foreach (self::$entries as $entry) {
            $cursor->appendRow($entry);
        }
        $table->commit();

        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $result = $statement->param_count();
        $this->assertEquals(3, $result);
    }

    public function testAffectedRows()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $statement = new Statement($this->fsql);
        $statement->prepare("INSERT INTO customers (personId, firstName, lastName, city, zip) VALUES (1, 'John', 'Smith', 'Los Angelos', 75677)");
        $passed = $statement->execute();
        $affectedRows = $statement->affected_rows();
        $this->assertTrue($affectedRows === 1);
    }

    public function testInsertId()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $statement = new Statement($this->fsql);
        $statement->prepare("INSERT INTO customers (firstName, lastName, city, zip) VALUES ('John', 'Smith', 'Los Angelos', 75677)");
        $passed = $statement->execute();
        $insertId = $statement->insert_id();
        $this->assertTrue($insertId === 3);
    }

    public function testFieldCountNoPrepare()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $statement = new Statement($this->fsql);
        $passed = $statement->field_count();
        $this->assertTrue($passed === false);
        $this->assertEquals($statement->error(), "Unable to perform a field_count without a prepare");
    }

    public function testFieldCount()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
        $statement = new Statement($this->fsql);
        $statement->prepare("SELECT firstName, lastName, city, zip FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
        $id = 5;
        $last = 'king';
        $zip = 99999;
        $statement->bind_param('isd', $id, $last, $zip);
        $results = $statement->execute();
        $fieldCount = $statement->field_count();
        $this->assertTrue($fieldCount === 4);
    }

    // public function testEnvPrepare()
    // {
    //     $dbName = 'db1';
    //     $passed = $this->fsql->define_db($dbName, parent::$tempDir);
    //     $this->fsql->select_db($dbName);
    //
    //     $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
    //     $cursor = $table->getWriteCursor();
    //     foreach (self::$entries as $entry) {
    //         $cursor->appendRow($entry);
    //     }
    //     $table->commit();
    //
    //     $expected = [
    //         ['stephen', 'king', 'derry'],
    //         ['bart', 'simpson', 'springfield'],
    //         [null, 'king', 'tokyo'],
    //     ];
    //
    //     $stmt = $this->fsql->prepare("SELECT firstName, lastName, city FROM customers WHERE personId = ? OR lastName = ? OR zip = ?");
    //     $this->assertTrue($passed !== false);
    //     $stmt->bind_param('is', '5', 'king');
    //     $passed = $stmt->execute();
    //     $this->assertTrue($passed !== false);
    //     $result = $stmt->get_result();
    //
    //     $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
    //     $this->assertEquals($expected, $results);
    // }
    //
    // public function testPrepareInject()
    // {
    //     $dbName = 'db1';
    //     $passed = $this->fsql->define_db($dbName, parent::$tempDir);
    //     $this->fsql->select_db($dbName);
    //
    //     $table = CachedTable::create($this->fsql->current_schema(), 'customers', self::$columns);
    //     $cursor = $table->getWriteCursor();
    //     foreach (self::$entries as $entry) {
    //         $cursor->appendRow($entry);
    //     }
    //     $table->commit();
    //
    //     $stmt = $this->fsql->prepare("SELECT firstName, lastName, city FROM customers WHERE lastName = ?");
    //     $this->assertTrue($stmt !== false);
    //     $stmt->bind_param('s', 'doe;delete from customers');
    //     $passed = $stmt->execute();
    //     $this->assertTrue($passed !== false);
    //     $result = $stmt->get_result();
    //
    //     $results = $this->fsql->fetch_all($result, ResultSet::FETCH_NUM);
    //     $this->assertSame([], $results);
    // }
}
