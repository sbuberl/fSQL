<?php

require_once __DIR__.'/SequenceBaseTest.php';

use FSQL\Database\Database;
use FSQL\Database\Sequence;
use FSQL\Database\SequencesFile;
use FSQL\Environment;

class SequenceTest extends SequenceBaseTest
{
    public function setUp()
    {
        parent::setUp();
        $fsql = new Environment();
        $database = new Database($fsql, 'db1', parent::$tempDir);
        $database->create();
        $schema = $database->getSchema('public');
        $sequences = new SequencesFile($schema);
        $sequences->create();
        $this->sequence = new Sequence('ids', $sequences);
    }

    public function testName()
    {
        $this->assertEquals('ids', $this->sequence->name());
    }
}
