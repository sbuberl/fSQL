<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

class fSQLSequenceTest extends fSQLBaseTest
{
    function setUp()
    {
        parent::setUp();
        $this->database =& new fSQLDatabase('db1', parent::$tempDir);
        $this->sequences =& new fSQLSequencesFile($this->database);
        $this->sequences->create();
    }

    function tearDown()
    {
        $this->sequences->close();
    }

    function testConstructor()
    {
        $name = 'shazam';
        $sequence =& new fSQLSequence($name, $this->sequences);

        $this->assertEquals($name, $sequence->name());
    }

    function testClose()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->close();
        $this->assertEmpty(get_object_vars($sequence));
    }

    function testSet()
    {
        $current = -14;
        $start = -2;
        $increment = -6;
        $min = -1000;
        $max = 0;
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set($current, $start, $increment, $min, $max, false);

        $this->assertEquals($sequence->current, $current);
        $this->assertEquals($sequence->start, $start);
        $this->assertEquals($sequence->increment, $increment);
        $this->assertEquals($sequence->min, $min);
        $this->assertEquals($sequence->max, $max);
        $this->assertFalse($sequence->cycle);
    }

    function testAlterErrors()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(12, 1, 2, 1, 1000, false);

        $result = $sequence->alter(array('INCREMENT' => 0));
        $this->assertEquals($result, 'Increment of zero in sequence/identity defintion is not allowed');

        $result = $sequence->alter(array('MINVALUE' => 100, 'MAXVALUE' => 6));
        $this->assertEquals($result, 'Sequence/identity minimum is greater than maximum');

        $result = $sequence->alter(array('RESTART' => 100, 'MINVALUE' => 0, 'MAXVALUE' => 6));
        $this->assertEquals($result, 'Sequence/identity restart value not between min and max');
    }

    function testAlterNothingInc()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(12, 3, 2, 1, 1000, false);

        $alterValues = array();
        $result = $sequence->alter(array());
        $this->assertTrue($result);
        $this->assertEquals($sequence->current, 1);
    }

    function testAlterNothingDec()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(12, 100, -4, 1, 200, false);

        $alterValues = array();
        $result = $sequence->alter(array());
        $this->assertTrue($result);
        $this->assertEquals($sequence->current, 200);
    }

    function testAlterAll()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(13, 1, 2, 1, 1000, false);

        $alterValues = array('RESTART' => 100, 'INCREMENT' => -5, 'MINVALUE' => 9, 'MAXVALUE' => 200, 'CYCLE' => 1);
        $result = $sequence->alter($alterValues);
        $this->assertTrue($result);
        $this->assertEquals($sequence->current, $alterValues['RESTART']);
        $this->assertEquals($sequence->increment, $alterValues['INCREMENT']);
        $this->assertEquals($sequence->min, $alterValues['MINVALUE']);
        $this->assertEquals($sequence->max, $alterValues['MAXVALUE']);
        $this->assertEquals($sequence->cycle, $alterValues['CYCLE']);
    }

    function testAlterStart()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(13, 3, 2, 1, 1000, false);

        $alterValues = array('RESTART' => 'start');
        $result = $sequence->alter($alterValues);
        $this->assertTrue($result);
        $this->assertEquals($sequence->current, 3);
    }

    function testNextValueForInc()
    {
        $current = 12;
        $increment = 2;
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set($current, 1, $increment, 1, 1000, false);

        $next = $sequence->nextValueFor();
        $this->assertEquals($next, $current);

        $next = $sequence->nextValueFor();
        $this->assertEquals($next, $current + $increment);
    }

    function testNextValueForDec()
    {
        $current = -100;
        $increment = -5;
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set($current, -15, $increment, -1000, 0, false);

        $next = $sequence->nextValueFor();


        $next = $sequence->nextValueFor();
        $this->assertEquals($next, $current + $increment);
    }

    function testNextValueForCycleInc()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(1, 1, 2, 1, 3, true);

        $this->assertEquals($sequence->nextValueFor(), 1);
        $this->assertEquals($sequence->nextValueFor(), 3);
        $this->assertEquals($sequence->nextValueFor(), 1);
    }

    function testNextValueForCycleDec()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(-1, -1, -2, -3, -1, true);

        $this->assertEquals($sequence->nextValueFor(), -1);
        $this->assertEquals($sequence->nextValueFor(), -3);
        $this->assertEquals($sequence->nextValueFor(), -1);
    }

    function testNextValueForBadCycle()
    {
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set(1, 1, 2, 1, 3, false);

        $this->assertEquals($sequence->nextValueFor(), 1);
        $this->assertEquals($sequence->nextValueFor(), 3);
        $this->assertFalse($sequence->nextValueFor());
    }

    function testRestart()
    {
        $current = 9;
        $start = 3;
        $sequence =& new fSQLSequence($name, $this->sequences);
        $sequence->set($current, $start, 1, 1, 1000, false);

        $sequence->restart();

        $this->assertEquals($sequence->current, $start);
    }
}

?>
