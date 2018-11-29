<?php

namespace FSQL\Statements;

use FSQL\Environment;

class CreateTableLike extends CreateTableBase
{
    const IDENTITY = 0;
    const DEFAULTS = 1;

    private $likeFullName;

    private $options;

    public function __construct(Environment $environment, array $destFullName, $ifNotExists, $temporary, array $likeFullName, array $options)
    {
        parent::__construct($environment, $destFullName, $ifNotExists, $temporary);
        $this->likeFullName = $likeFullName;
        $this->options = $options;
    }

    public function execute()
    {
        $tableName = $this->fullName[2];

        $schema = $this->environment->find_schema($this->fullName[0], $this->fullName[1]);
        if ($schema === false) {
            return false;
        }

        $table = $this->getRelation($schema, $tableName);
        if (is_bool($table)) {
            return $table;
        }

        $likeTable = $this->environment->find_table($this->likeFullName);
        if ($likeTable !== false) {
            $likeColumns = $likeTable->getColumns();
        } else {
            return false;
        }

        foreach ($this->options as $type => $including) {
            if ($type === static::IDENTITY) {
                $identity = $likeTable->getIdentity();
                if ($identity !== false) {
                    $identityColumn = $identity->getColumnName();
                    if ($including) {
                        $likeColumns[$identityColumn]['restraint'][0] = $likeColumns[$identityColumn]['restraint'][2];
                    } else {
                        $likeColumns[$identityColumn]['auto'] = 0;
                        $likeColumns[$identityColumn]['restraint'] = array();
                    }
                }
            } else {  // DEFAULTS
                if (!$including) {
                    foreach ($likeColumns as &$likeColumn) {
                        $likeColumn['default'] = $this->environment->get_type_default_value($likeColumn['type'], $likeColumn['null']);
                    }
                }
            }
        }

        $schema->createTable($tableName, $likeColumns, $this->temporary);

        return true;
    }
}
