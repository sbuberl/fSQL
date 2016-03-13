<?php

require_once dirname(__FILE__) . '/fSQLSequenceBaseTest.php';

class fSQLSequenceTest extends fSQLSequenceBaseTest
{
    public function setUp()
    {
        parent::setUp();
        $fsql = new fSQLEnvironment();
        $database = new fSQLDatabase($fsql, 'db1', parent::$tempDir);
        $database->create();
        $schema = $database->getSchema('public');
        $sequences = new fSQLSequencesFile($schema);
        $sequences->create();
        $this->sequence = new fSQLSequence('ids', $sequences);
    }

    public function testName()
    {
        $this->assertEquals('ids', $this->sequence->name());
    }
}
