<?php

require_once dirname(__FILE__) . '/fSQLBaseTest.php';

abstract class fSQLSequenceBaseTest extends fSQLBaseTest
{
    protected $sequence;

     function testSet()
    {
        $current = -14;
        $start = -2;
        $increment = -6;
        $min = -1000;
        $max = 0;
        $this->sequence->set($current, $start, $increment, $min, $max, false);
        $this->sequence->save();

        $this->assertEquals($this->sequence->current, $current);
        $this->assertEquals($this->sequence->start, $start);
        $this->assertEquals($this->sequence->increment, $increment);
        $this->assertEquals($this->sequence->min, $min);
        $this->assertEquals($this->sequence->max, $max);
        $this->assertFalse($this->sequence->cycle);
    }

    function testAlterErrors()
    {
        $this->sequence->set(12, 1, 2, 1, 1000, false);
        $this->sequence->save();

        $result = $this->sequence->alter(array('INCREMENT' => 0));
        $this->assertEquals($result, 'Increment of zero in sequence/identity defintion is not allowed');

        $result = $this->sequence->alter(array('MINVALUE' => 100, 'MAXVALUE' => 6));
        $this->assertEquals($result, 'Sequence/identity minimum is greater than maximum');

        $result = $this->sequence->alter(array('RESTART' => 100, 'MINVALUE' => 0, 'MAXVALUE' => 6));
        $this->assertEquals($result, 'Sequence/identity restart value not between min and max');
    }

    function testAlterAll()
    {
        $this->sequence->set(13, 1, 2, 1, 1000, false);
        $this->sequence->save();

        $alterValues = array('RESTART' => 100, 'INCREMENT' => -5, 'MINVALUE' => 9, 'MAXVALUE' => 200, 'CYCLE' => 1);
        $result = $this->sequence->alter($alterValues);
        $this->assertTrue($result);
        $this->assertEquals($this->sequence->current, $alterValues['RESTART']);
        $this->assertEquals($this->sequence->increment, $alterValues['INCREMENT']);
        $this->assertEquals($this->sequence->min, $alterValues['MINVALUE']);
        $this->assertEquals($this->sequence->max, $alterValues['MAXVALUE']);
        $this->assertEquals($this->sequence->cycle, $alterValues['CYCLE']);
    }

    function testAlterStart()
    {
        $this->sequence->set(13, 3, 2, 1, 1000, false);
        $this->sequence->save();

        $alterValues = array('RESTART' => 'start');
        $result = $this->sequence->alter($alterValues);
        $this->assertTrue($result);
        $this->assertEquals($this->sequence->current, 3);
    }

    function testNextValueForInc()
    {
        $current = 12;
        $increment = 2;
        $this->sequence->set($current, 1, $increment, 1, 1000, false);
        $this->sequence->save();

        $next = $this->sequence->nextValueFor();
        $this->assertEquals($next, $current);

        $next = $this->sequence->nextValueFor();
        $this->assertEquals($next, $current + $increment);
    }

    function testNextValueForDec()
    {
        $current = -100;
        $increment = -5;
        $this->sequence->set($current, -15, $increment, -1000, 0, false);
        $this->sequence->save();

        $next = $this->sequence->nextValueFor();

        $next = $this->sequence->nextValueFor();
        $this->assertEquals($next, $current + $increment);
    }

    function testNextValueForCycleInc()
    {
        $this->sequence->set(1, 1, 2, 1, 3, true);
        $this->sequence->save();

        $this->assertEquals($this->sequence->nextValueFor(), 1);
        $this->assertEquals($this->sequence->nextValueFor(), 3);
        $this->assertEquals($this->sequence->nextValueFor(), 1);
    }

    function testNextValueForCycleDec()
    {
        $this->sequence->set(-1, -1, -2, -3, -1, true);
        $this->sequence->save();

        $this->assertEquals($this->sequence->nextValueFor(), -1);
        $this->assertEquals($this->sequence->nextValueFor(), -3);
        $this->assertEquals($this->sequence->nextValueFor(), -1);
    }

    function testNextValueForBadCycle()
    {
        $this->sequence->set(1, 1, 2, 1, 3, false);
        $this->sequence->save();

        $this->assertEquals($this->sequence->nextValueFor(), 1);
        $this->assertEquals($this->sequence->nextValueFor(), 3);
        $this->assertFalse($this->sequence->nextValueFor());
    }

    function testRestart()
    {
        $current = 9;
        $start = 3;
        $this->sequence->set($current, $start, 1, 1, 1000, false);
        $this->sequence->save();

        $this->sequence->restart();

        $this->assertEquals($this->sequence->current, $start);
    }
}

?>
