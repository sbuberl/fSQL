<?php

namespace FSQL\Queries;

use FSQL\Environment;

class Lock extends Query
{
    private $locks;

    public function  __construct(Environment $environment, array $locks)
    {
        parent:: __construct($environment);
        $this->locks = $locks;
    }

    public function execute()
    {
        foreach($this->locks as $lock) {
            list($tableName, $isRead) = $lock;
            $table = $this->environment->find_table($tableName);
            if ($table === false) {
                return false;
            }

            if ($isRead) {
                $table->readLock();
            } else {
                $table->writeLock();
            }

            $this->environment->lockedTables[] = $table;
        }

        return true;
    }
}
