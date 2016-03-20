<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Database\Database;
use FSQL\Database\SequencesFile;
use FSQL\Environment;

class SequenceFileTest extends BaseTest
{
    private $schema;
    private $sequences;

    public function setUp()
    {
        parent::setUp();
        $fsql = new Environment();
        $database = new Database($fsql, 'db1', parent::$tempDir);
        $database->create();

        $this->schema = $database->getSchema('public');
        $this->sequences = new SequencesFile($this->schema);
    }

    public function testConstructor()
    {
        $this->assertEquals($this->sequences->schema()->name(), $this->schema->name());
        $this->assertTrue($this->sequences->isEmpty());
    }

    public function testCreate()
    {
        $result = $this->sequences->create();
        $this->assertTrue($result);
        $this->assertTrue($this->sequences->lockFile->exists());
        $this->assertTrue($this->sequences->exists());
    }

    public function testDrop()
    {
        $result = $this->sequences->create();
        $this->assertTrue($this->sequences->exists());

        $this->sequences->drop();
        $this->assertFalse($this->sequences->exists());
    }

    public function testExists()
    {
        $this->assertFalse($this->sequences->exists());

        $this->sequences->create();
        $this->assertTrue($this->sequences->exists());
    }

    public function testAddThenGet()
    {
        $this->sequences->create();
        $this->sequences->addSequence('seq12', 3, 2, 0, 100, true);

        $sequence = $this->sequences->getSequence('seq12');
        $this->assertEquals($sequence->name(), 'seq12');
        $this->assertEquals($sequence->current, 3);
        $this->assertEquals($sequence->start, 3);
        $this->assertEquals($sequence->increment, 2);
        $this->assertEquals($sequence->min, 0);
        $this->assertEquals($sequence->max, 100);
        $this->assertTrue($sequence->cycle);
    }

    public function testGetEmpty()
    {
        $this->sequences->create();
        $sequence = $this->sequences->getSequence('seq12');
        $this->assertFalse($sequence);
    }

    public function testAddThenDrop()
    {
        $this->sequences->create();
        $this->sequences->addSequence('seq12474', 5, 1, 5, 1000, false);

        $this->sequences->dropSequence('seq12474');

        $sequence = $this->sequences->getSequence('seq12474');
        $this->assertFalse($sequence);
    }
}
