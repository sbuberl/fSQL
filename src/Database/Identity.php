<?php

namespace FSQL\Database;

class Identity extends SequenceBase
{
    private $table;
    private $columnName;
    private $always;

    public function __construct(Table $table, $columnName)
    {
        parent::__construct($table->columnsLockFile);
        $this->table = $table;
        $this->columnName = $columnName;
    }

    public function getColumnName()
    {
        return $this->columnName;
    }

    public function getAlways()
    {
        $this->load();

        return $this->always;
    }

    public function load()
    {
        $columns = $this->table->getColumns();
        $identity = $columns[$this->columnName]['restraint'];
        list($current, $always, $start, $increment, $min, $max, $cycle) = $identity;
        $this->always = $always;
        $this->set($current, $start, $increment, $min, $max, $cycle);
    }

    public function save()
    {
        $columns = $this->table->getColumns();
        $columns[$this->columnName]['restraint'] = array($this->current, $this->always,
            $this->start, $this->increment, $this->min, $this->max, $this->cycle, );
        $this->table->setColumns($columns);
    }

    public function alter(array $updates)
    {
        if (array_key_exists('ALWAYS', $updates)) {
            $this->always = (int) $updates['ALWAYS'];
        }

        return parent::alter($updates);
    }
}
