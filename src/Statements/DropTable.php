<?php

namespace FSQL\Statements;

use FSQL\Environment;

class DropTable extends DropRelationBase
{
    public function execute()
    {
        $tableName = $this->fullName[2];

        $schema = $this->environment->find_schema($this->fullName[0], $this->fullName[1]);
        if ($schema === false) {
            return false;
        }

        $table = $this->getRelation($schema, $tableName);
        if ($table === true) {
            return true;
        } elseif ($table === false) {
            return $this->environment->error_table_not_exists($this->fullName);
        }

        if ($table->isReadLocked()) {
            return $this->environment->error_table_read_lock($table_name_pieces);
        }

        return $schema->dropTable($tableName);
    }
}