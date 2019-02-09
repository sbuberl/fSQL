<?php

namespace FSQL\Statements;

use FSQL\Environment;

class RenameTable extends Statement
{
    private $oldName;
    private $newName;

    public function  __construct(Environment $environment, array $oldName, array $newName)
    {
        parent:: __construct($environment);
        $this->oldName = $oldName;
        $this->newName = $newName;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->oldName);
        if ($table === false) {
            return false;
        }
        $schema = $table->schema();

        $newSchema = $this->environment->find_schema($this->newName[0], $this->newName[1]);
        if ($newSchema === false) {
            return false;
        }

        $newTable = $newSchema->getTable($this->newName[2]);
        if ($newTable->exists()) {
            return $this->environment->set_error("Destination table {$newTable->fullName()} already exists");
        }

        return $schema->renameTable($table->name(), $newTable->name(), $newSchema);
    }
}
