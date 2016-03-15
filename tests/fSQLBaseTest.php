<?php

require_once dirname(__FILE__).'/../fSQL.php';

error_reporting(E_ALL);

abstract class fSQLBaseTest extends PHPUnit_Framework_TestCase
{
    public static $tempDir = '.tmp/';

    public function setUp()
    {
        if (file_exists(self::$tempDir)) {
            fsql_delete_directory(self::$tempDir);
        }
        mkdir(self::$tempDir);
    }
}
