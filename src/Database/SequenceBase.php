<?php

namespace FSQL\Database;

use FSQL\MicrotimeLockFile;

abstract class SequenceBase
{
    private $lockFile;
    public $current;
    public $start;
    public $increment;
    public $min;
    public $max;
    public $cycle;
    protected $lastValue = null;

    public function __construct(MicrotimeLockFile $lockFile)
    {
        $this->lockFile = $lockFile;
    }

    public function lastValue()
    {
        return $this->lastValue;
    }

    abstract public function load();

    abstract public function save();

    private function lockAndReload()
    {
        $this->lockFile->acquireWrite();
        if ($this->lockFile->wasModified()) {
            $this->load();
        }
    }

    private function saveAndUnlock()
    {
        $this->save();

        $this->lockFile->releaseWrite();
    }

    public function set($current, $start, $increment, $min, $max, $cycle)
    {
        $this->current = $current;
        $this->start = $start;
        $this->increment = $increment;
        $this->min = $min;
        $this->max = $max;
        $this->cycle = $cycle;
    }

    public function alter(array $updates)
    {
        $this->lockAndReload();

        if (array_key_exists('INCREMENT', $updates)) {
            $this->increment = (int) $updates['INCREMENT'];
            if ($this->increment === 0) {
                $this->lockFile->releaseWrite();

                return 'Increment of zero in sequence/identity defintion is not allowed';
            }
        }

        $climbing = $this->increment > 0;
        if (array_key_exists('MINVALUE', $updates)) {
            $this->min = isset($updates['MINVALUE']) ? (int) $updates['MINVALUE'] : ($climbing ? 1 : PHP_INT_MIN);
        }
        if (array_key_exists('MAXVALUE', $updates)) {
            $this->max = isset($updates['MAXVALUE']) ? (int) $updates['MAXVALUE'] : ($climbing ? PHP_INT_MAX : -1);
        }
        if (array_key_exists('CYCLE', $updates)) {
            $this->cycle = isset($updates['CYCLE']) ? (int) $updates['CYCLE'] : 0;
        }

        if ($this->min > $this->max) {
            $this->lockFile->releaseWrite();

            return 'Sequence/identity minimum is greater than maximum';
        }

        if (isset($updates['RESTART'])) {
            $restart = $updates['RESTART'];
            $this->current = $restart !== 'start' ? (int) $restart : $this->start;
            if ($this->current < $this->min || $this->current > $this->max) {
                $this->lockFile->releaseWrite();

                return 'Sequence/identity restart value not between min and max';
            }
        }

        $this->saveAndUnlock();

        return true;
    }

    public function nextValueFor()
    {
        $this->lockAndReload();

        $cycled = false;
        if ($this->increment > 0 && $this->current > $this->max) {
            $this->current = $this->min;
            $cycled = true;
        } elseif ($this->increment < 0 && $this->current < $this->min) {
            $this->current = $this->max;
            $cycled = true;
        }

        if ($cycled && !$this->cycle) {
            $this->lockFile->releaseWrite();

            return false;
        }

        $current = $this->current;
        $this->lastValue = $current;
        $this->current += $this->increment;

        $this->saveAndUnlock();

        return $current;
    }

    public function restart()
    {
        $this->lockAndReload();

        $this->current = $this->start;

        $this->saveAndUnlock();
    }
}
