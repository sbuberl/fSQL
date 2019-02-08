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
            $this->entries[$rowId] = array_replace($this->entries[$rowId], $updates);

            // if row is not new in this transaction,
            // add updates to updatedRows array.
            if($key = array_search($rowId, $this->newRows) === FALSE)
            {
                if(!isset($this->updatedRows[$rowId]))
                    $this->updatedRows[$rowId] = [];
                $this->updatedRows[$rowId] = array_replace($this->updatedRows[$rowId], $updates);
            }

            $keys = $this->table->getKeys();
            foreach($keys as $key) {
                $idx = $key->extractIndex($this->entries[$rowId]);
                $key->updateEntry($rowId, $idx);
            }
        }
    }

    public function deleteColumn($column)
    {
        $rowId = $this->currentRowId;
        if($rowId !== false) {
            unset($this->entries[$rowId][$column]);
            if(!isset($this->updatedRows[$rowId]))
                $this->updatedRows[$rowId] = [];
            $this->updatedRows[$rowId] = [];
        }
        return false;
    }

    public function deleteRow()
    {
        $rowId = $this->currentRowId;
        if($rowId !== false) {
            if($key = array_search($rowId, $this->newRows) === FALSE)  // row added in same transaction
                unset($this->newRows[$key]);
            else if(isset($this->updatedRows[$rowId]))
                unset($this->updatedRows[$rowId]);

            if($key = array_search($rowId, $this->deletedRows) === FALSE) // double check not already in there
                $this->deletedRows[] = $rowId;

            $keys = $this->table->getKeys();
            foreach($keys as $key)
            {
                $key->deleteEntry($rowId);
            }

            --$this->numRows;
            unset($this->entries[$rowId]);
            $this->currentRowId = key($this->entries);
            if($this->currentRowId === null) { // key on an empty array is null?
                $this->currentRowId = false;
                $this->entries = [];
                return false;
            }
            return true;
        }
        return false;
    }

    function isUncommitted()
    {
        return !empty($this->newRows) || !empty($this->updatedRows) || !empty($this->deletedRows);
    }
}
