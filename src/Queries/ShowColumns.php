<?php

namespace FSQL\Queries;

use FSQL\Environment;
use FSQL\Types;
use FSQL\ResultSet;

class ShowColumns extends Query
{
    private $fullName;
    private $full;

    public function  __construct(Environment $environment, array $fullName, $full)
    {
        parent:: __construct($environment);
        $this->fullName = $fullName;
        $this->full = $full;
    }

    public function execute()
    {
        $table = $this->environment->find_table($this->fullName);
        if ($table === false) {
            return false;
        }
        $tableColumns = $table->getColumns();

        $data = [];

        foreach ($tableColumns as $name => $column) {
            $type = Types::getTypeName($column['type']);
            $null = ($column['null']) ? 'YES' : 'NO';
            $extra = ($column['auto']) ? 'auto_increment' : '';
            $default = $column['default'];

            if (preg_match("/\A'(.*?(?<!\\\\))'\Z/is", $default, $matches)) {
                $default = $matches[1];
            }

            if ($column['key'] == 'p') {
                $key = 'PRI';
            } elseif ($column['key'] == 'u') {
                $key = 'UNI';
            } else {
                $key = '';
            }

            $row = [$name, $type, $null, $default, $key, $extra];
            if ($this->full) {
                array_splice($row, 2, 0, [null]);
                array_push($row, 'select,insert,update,references', '');
            }

            $data[] = $row;
        }

        $columns = ['Field', 'Type', 'Null', 'Default', 'Key', 'Extra'];
        if ($this->full) {
            array_splice($columns, 2, 0, 'Collation');
            array_push($columns, 'Privileges', 'Comment');
        }

        return new ResultSet($columns, $data);
    }
}