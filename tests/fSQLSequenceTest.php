<?php

require_once dirname(__FILE__) . '/fSQLSequenceBaseTest.php';

class fSQLSequenceTest extends fSQLSequenceBaseTest
{
    public function setUp()
    {
        parent::setUp();
        $database = new fSQLDatabase('db1', parent::$tempDir);
        $sequences = new fSQLSequencesFile($database);
        $sequences->create();
        $this->sequence = new fSQLSequence('ids', $sequences);
    }

    public function testName()
    {
        $this->assertEquals('ids', $this->sequence->name());
    }
}
