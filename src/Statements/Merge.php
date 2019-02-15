<?php

namespace FSQL\Statements;

use FSQL\Environment;
use FSQL\Utilities;

class Merge extends DataModifyStatement
{
    private $targetTableName;
    private $sourceTableName;
    private $matched;
    private $unmatched;
    private $joinFunction;

    public function __construct(Environment $environment, array $targetTable, array $sourceTable, array $matched, array $unmatched, $joinFunction)
    {
        parent::__construct($environment);
        $this->targetTableName = $targetTable;
        $this->sourceTableName = $sourceTable;
        $this->matched = $matched;
        $this->unmatched = $unmatched;
        $this->joinFunction = $joinFunction;
    }

    public function execute()
    {
        $this->affected = 0;

        if (!($destTable = $this->environment->find_table($this->targetTableName))) {
            return false;
        }

        if (!($sourceTable = $this->environment->find_table($this->sourceTableName))) {
            return false;
        }

        $sourceTableColumns = $sourceTable->getColumns();
        $sourceColumnSize = count($sourceTableColumns);
        $joinMatches = [];
        Utilities::leftJoin($sourceTable->getEntries(), $destTable->getEntries(), $this->joinFunction, $sourceColumnSize, $joinMatches);

        $hasNotMatched = !empty($this->unmatched);
        $hasMatched = !empty($this->matched);

        $srcCursor = $sourceTable->getWriteCursor();
        $destCursor = $destTable->getWriteCursor();
        foreach ($srcCursor as $srcRowId => $entry) {
            $destRowId = $joinMatches[$srcRowId];
            if ($destRowId === false && $hasNotMatched) {
                foreach($this->unmatched as $unmatchedCode) {
                    $newRow = eval($unmatchedCode);
                    if ($newRow !== false) {
                        $destCursor->appendRow($newRow);
                        ++$this->affected;
                    }
                }
            } else if($destRowId !== false && $hasMatched) {
                $found = $destCursor->findKey($destRowId);
                foreach($this->matched as $matched) {
                    list($updateCode, $isDelete) = $matched;
                    if(!$isDelete) {
                        $updates = eval($updateCode);
                        if ($updates !== false) {
                            $destCursor->updateRow($updates);
                            ++$this->affected;
                        }
                    } else {
                        $updates = eval($updateCode);
                        ++$this->affected;
                    }
                }
            }
        }

        return true;
    }
}
