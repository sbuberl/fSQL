<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\SequencesFile;
use FSQL\Environment;

class CreateSequenceTest extends BaseTest
{
    private $fsql;
    private $sequences;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
        $this->sequences = new SequencesFile($this->fsql->current_schema());
    }

    public function testWrongDB()
    {
        $dbName = 'wrongDB';
        $result = $this->fsql->query("CREATE SEQUENCE $dbName.public.userids MINVALUE 11;");
        $this->assertFalse($result);
        $this->assertEquals("Database $dbName not found", trim($this->fsql->error()));
    }

    public function testFoundError()
    {
        $fullName = 'db1.public.userids';
        $this->sequences->addSequence('userids', 1, 1, 1, 10000, false);

        $result = $this->fsql->query("CREATE SEQUENCE $fullName MINVALUE 12;");
        $this->assertFalse($result);
        $this->assertEquals("Relation $fullName already exists", trim($this->fsql->error()));
    }

    public function testFoundIgnore()
    {
        $this->sequences->addSequence('userids', 1, 1, 1, 10000, false);

        $result = $this->fsql->query('CREATE SEQUENCE IF NOT EXISTS userids MINVALUE 13;');
        $this->assertTrue($result);
        $this->assertNotNull($this->sequences->getSequence('userids'));
    }

    public function testParseParamsError()
    {
        $result = $this->fsql->query('CREATE SEQUENCE userids MINVALUE 10 MINVALUE 32;');
        $this->assertFalse($result);
        $this->assertEquals('MINVALUE already set for this identity/sequence.', trim($this->fsql->error()));
    }

    public function testSuccess()
    {
        $result = $this->fsql->query('CREATE SEQUENCE userids INCREMENT BY 2 START WITH 22 MAXVALUE 66 NO CYCLE');
        $this->assertTrue($result);

        $sequence = $this->sequences->getSequence('userids');
        $this->assertEquals(2, $sequence->increment);
        $this->assertEquals(22, $sequence->nextValueFor());
        $this->assertEquals(66, $sequence->max);
        $this->assertEquals(false, $sequence->cycle);
    }
}
