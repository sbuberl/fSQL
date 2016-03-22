<?php

require_once __DIR__.'/SequenceBaseTest.php';

use FSQL\Database\Database;
use FSQL\Database\Sequence;
use FSQL\Database\SequencesFile;
use FSQL\Environment;

class SequenceTest extends SequenceBaseTest
{
    private $schema;

    public function setUp()
    {
        parent::setUp();
        $fsql = new Environment();
        $database = new Database($fsql, 'db1', parent::$tempDir);
        $database->create();
        $this->schema = $database->getSchema('public');
        $sequences = new SequencesFile($this->schema);
        $sequences->create();
        $this->sequence = new Sequence('ids', $sequences);
    }

    public function testName()
    {
        $this->assertEquals('ids', $this->sequence->name());
    }

    public function testFullName()
    {
        $this->assertEquals($this->schema->fullName().'.ids', $this->sequence->fullName());
    }

    public function testDrop()
    {
        $this->sequence->drop();

        $sequence = $this->schema->getSequence('ids');
        $this->assertFalse($sequence);
    }
}
