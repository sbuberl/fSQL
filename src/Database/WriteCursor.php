<?php

namespace FSQL\Database;

class WriteCursor extends TableCursor
{
    private $table;
    private $newRows = [];
    private $updatedRows = [];
    private $deletedRows = [];

    public function __construct(array &$entries, Table $table)
    {
        parent::__construct($entries);
        $this->table = $table;
    }

    public function getNewRows()
    {
        return array_intersect_key($this->entries, array_flip($this->newRows));
    }

    public function appendRow(array $entry, $rowId = false)
    {
        if($rowId !== false) {
            $this->entries[$rowId] = $entry;
        } else {
            $this->entries[] = $entry;
            $aKeys = array_keys($this->entries);
            $rowId = end($aKeys);
        }

        ++$this->numRows;
        $keys = $this->table->getKeys();
        if(!empty($keys)) {
            foreach($keys as $key) {
                $idx = $key->extractIndex($entry);
                $key->addEntry($rowId, $idx);
            }
        }

        $this->newRows[] = $rowId;
        return $rowId;
    }

    public function updateRow(array $updates)
    {
        $rowId = $this->currentRowId;
        if($rowId !== false) {
            foreach($updates as $column => $value)
                $this->entries[$rowId][$column] = $value;

            // if row is not new in this transaction,
            // add updates to updatedRows array.
            if(!isset($this->newRows[$rowId]))
            {
                if(!isset($this->updatedRows[$rowId]))
                $this->updatedRows[$rowId] = [];

                foreach($updates as $column => $value)
                $this->updatedRows[$rowId][$column] = $value;
            }

            $keys = $this->table->getKeys();
            foreach($keys as $key) {
                $idx = $key->extractIndex($this->entries[$rowId]);
                $key->updateEntry($rowId, $idx);
            }
        }
    }

    public function deleteRow()
    {
        $rowId = $this->currentRowId;
        if($rowId !== false) {
            if(isset($this->newRows[$rowId])) // row added in same transaction
                unset($this->newRows[$rowId]);
            else if(isset($this->updatedRows[$rowId]))
                unset($this->updatedRows[$rowId]);
            else if(!in_array($rowId, $this->deletedRows)) // double check not already in there
                $this->deletedRows[] = $rowId;

            --$this->numRows;
            unset($this->entries[$rowId]);
            $this->currentRowId = key($this->entries);
            if($this->currentRowId === null) { // key on an empty array is null?
                $this->currentRowId = false;
                $this->entries = [];
            }

            $keys = $this->table->getKeys();
            foreach($keys as $key)
            {
                $key->deleteEntry($rowId);
            }
        }
    }

    function isUncommitted()
    {
        return !empty($this->newRows) || !empty($this->updatedRows) || !empty($this->deletedRows);
    }
}
