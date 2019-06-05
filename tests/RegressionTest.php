p[olate<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Environment;
use FSQL\ResultSet;

class RegressionTest extends BaseTest
{
    private $fsql;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function tearDown()
    {
        unset($this->fsql);
    }

    public function testIssue23()
    {
        $createPassed = $this->fsql->query("CREATE TABLE fruits (ID INTEGER AUTO_INCREMENT PRIMARY KEY, fruit TEXT, amount INT)");
        $this->assertTrue($createPassed);

        $populatePassed = $this->fsql->query("INSERT INTO fruits (fruit, amount) VALUES ('Strawberry', 22), ('Apple', 22), ('Peach', 22), ('Plum', 42), ('Mango', 42), ('Cherry', 22), ('Orange', 68), ('Lemon', 62), ('Lime', 50), ('Banana', 18);");
        $this->assertTrue($populatePassed);

        $populateResults= $this->fsql->query("SELECT * FROM fruits");
        $this->assertTrue($populateResults !== false);

        $expectedAfterPopulate = [
            [1,'Strawberry',22],
            [2,'Apple',22],
            [3,'Peach',22],
            [4,'Plum',42],
            [5,'Mango',42],
            [6,'Cherry',22],
            [7,'Orange',68],
            [8,'Lemon',62],
            [9,'Lime',50],
            [10,'Banana',18],
        ];
        $populateData = $populateResults->fetch_all(ResultSet::FETCH_NUM);
        $this->assertEquals($expectedAfterPopulate, $populateData);

        $deletePassed = $this->fsql->query("DELETE FROM fruits WHERE fruit IN ('Plum', 'Mango', 'Orange', 'Lemon')");
        $this->assertTrue($deletePassed);

        $deleteResults = $this->fsql->query("SELECT * FROM fruits");
        $this->assertTrue($deleteResults !== false);

        $expectedAfterDelete = [
            [1,'Strawberry',22],
            [2,'Apple',22],
            [3,'Peach',22],
            [6,'Cherry',22],
            [9,'Lime',50],
            [10,'Banana',18],
        ];
        $deleteData = $deleteResults->fetch_all(ResultSet::FETCH_NUM);
        $this->assertEquals($expectedAfterDelete, $deleteData);

        $shipmentPassed = $this->fsql->query("INSERT INTO fruits (fruit, amount) VALUES ('Pineapple', 100), ('Orange', 100), ('Watermelon', 100);");
        $this->assertTrue($shipmentPassed);

        $shipmentResults = $this->fsql->query("SELECT * FROM fruits");
        $expectedAfterShipment = [
            [1,'Strawberry',22],
            [2,'Apple',22],
            [3,'Peach',22],
            [6,'Cherry',22],
            [9,'Lime',50],
            [10,'Banana',18],
            [11,'Pineapple',100],
            [12,'Orange',100],
            [13,'Watermelon',100],
        ];
        $shipmentData = $shipmentResults->fetch_all(ResultSet::FETCH_NUM);
        $this->assertEquals($expectedAfterShipment, $shipmentData);
    }
}

