<?php

require 'vendor/autoload.php';

error_reporting(E_ALL);

use FSQL\Utilities;

abstract class BaseTest extends PHPUnit_Framework_TestCase
{
    public static $tempDir = '.tmp/';

    public function setUp()
    {
        if (file_exists(self::$tempDir)) {
            Utilities::deleteDirectory(self::$tempDir);
        }
        mkdir(self::$tempDir);
    }
}
