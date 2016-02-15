<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class fSQLFunctionsTest extends fSQLBaseTest
{
    private $fsql;
    private $functions;

    function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
        $this->fsql->define_db("db", parent::$tempDir);
        $this->fsql->select_db("db");
        $this->functions = new fSQLFunctions($this->fsql);
    }

    function testNextval()
    {
        $name = "counter";
        $db = $this->fsql->current_db();
        $sequences = $db->getSequences();
        $sequences->addSequence($name, 3, 1, 1, 100, false);

        $this->assertEquals(3, $this->functions->nextval($name));
        $this->assertEquals(4, $this->functions->nextval($name));
        $this->assertEquals(5, $this->functions->nextval($name));
    }

    function testCurval()
    {
        $name = "counter";
        $db = $this->fsql->current_db();
        $sequences = $db->getSequences();
        $sequences->addSequence($name, -1, -2, -100, 1, false);

        $this->functions->nextval($name);
        $this->assertEquals(-1, $this->functions->currval($name));
        $this->functions->nextval($name);
        $this->assertEquals(-3, $this->functions->currval($name));
        $this->functions->nextval($name);
        $this->assertEquals(-5, $this->functions->currval($name));
    }
}

?>
