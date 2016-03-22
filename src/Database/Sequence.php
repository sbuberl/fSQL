<?php

namespace FSQL\Database;

class Sequence extends SequenceBase implements Relation
{
    private $name;
    private $file;

    public function __construct($name, SequencesFile $file)
    {
        parent::__construct($file->lockFile);
        $this->name = $name;
        $this->file = $file;
    }

    public function name()
    {
        return $this->name;
    }

    public function drop()
    {
        return $this->file->dropSequence($this->name);
    }

    public function fullName()
    {
        return $this->file->schema()->fullName().'.'.$this->name;
    }

    public function load()
    {
        $this->file->reload();
    }

    public function save()
    {
        $this->file->save();
    }
}
