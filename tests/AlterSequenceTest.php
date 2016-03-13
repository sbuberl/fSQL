<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class AlterSequenceTest extends fSQLBaseTest
{
    private $fsql;
    private $sequences;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new fSQLEnvironment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $this->sequences = new fSQLSequencesFile($this->fsql->current_schema());
    }

    public function testWrongDB()
    {
        $dbName = "wrongDB";
        $result = $this->fsql->query("ALTER SEQUENCE $dbName.public.userids MINVALUE=11;");
        $this->assertFalse($result);
        $this->assertEquals("Database $dbName not found", trim($this->fsql->error()));
    }

    public function testWrongSchema()
    {
        $schemaName = "wrongSchema";
        $result = $this->fsql->query("ALTER SEQUENCE $schemaName.userids MINVALUE=11;");
        $this->assertFalse($result);
        $this->assertEquals("Schema db1.$schemaName does not exist", trim($this->fsql->error()));
    }

    public function testNotFoundError()
    {
        $fullName = "userids";
        $result = $this->fsql->query("ALTER SEQUENCE $fullName MINVALUE=12;");
        $this->assertFalse($result);
        $this->assertEquals("Sequence db1.public.{$fullName} does not exist", trim($this->fsql->error()));
    }

    public function testNotFoundIgnore()
    {
        $result = $this->fsql->query("ALTER SEQUENCE IF EXISTS userids MINVALUE=13;");
        $this->assertTrue($result);
        $this->assertFalse($this->sequences->getSequence('userids'));
    }

    public function testParseParamsError()
    {
        $this->sequences->addSequence("userids", 1, 1, 1, 10000, false);

        $result = $this->fsql->query("ALTER SEQUENCE userids MINVALUE 10 MINVALUE 32;");
        $this->assertFalse($result);
        $this->assertEquals("MINVALUE already set for this identity/sequence.", trim($this->fsql->error()));
    }

    public function testSuccess()
    {
        $this->sequences->addSequence("userids", 1, 1, 1, 10000, false);

        $result = $this->fsql->query("ALTER SEQUENCE userids INCREMENT BY 3 RESTART WITH 50");
        $this->assertTrue($result);

        $sequence = $this->sequences->getSequence('userids');
        $this->assertEquals(3, $sequence->increment);
        $this->assertEquals(50, $sequence->nextValueFor());
    }
}
