<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\Environment;
use FSQL\Functions;

class FunctionsTest extends BaseTest
{
    private $fsql;
    private $functions;

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db', parent::$tempDir);
        $this->fsql->select_db('db');
        $this->functions = new Functions($this->fsql);
    }

    public function testCurrentCatalogNotSet()
    {
        $fsql = new Environment();
        $functions = new Functions($fsql);
        $this->assertNull($functions->current_catalog());
    }

    public function testCurrentCatalog()
    {
        $this->assertEquals('db', $this->functions->current_catalog());
    }

    public function testCurrentSchemaNotSet()
    {
        $fsql = new Environment();
        $functions = new Functions($fsql);
        $this->assertNull($functions->current_schema());
    }

    public function testCurrentSchema()
    {
        $this->assertEquals('public', $this->functions->current_schema());
    }

    public function testNextval()
    {
        $name = 'counter';
        $schema = $this->fsql->current_schema();
        $sequences = $schema->getSequences();
        $sequences->addSequence($name, 3, 1, 1, 100, false);

        $this->assertEquals(3, $this->functions->nextval($name));
        $this->assertEquals(4, $this->functions->nextval($name));
        $this->assertEquals(5, $this->functions->nextval($name));
    }

    public function testCurval()
    {
        $name = 'counter';
        $schema = $this->fsql->current_schema();
        $sequences = $schema->getSequences();
        $sequences->addSequence($name, -1, -2, -100, 1, false);

        $this->functions->nextval($name);
        $this->assertEquals(-1, $this->functions->currval($name));
        $this->functions->nextval($name);
        $this->assertEquals(-3, $this->functions->currval($name));
        $this->functions->nextval($name);
        $this->assertEquals(-5, $this->functions->currval($name));
    }
}
