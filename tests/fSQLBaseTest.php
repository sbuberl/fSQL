<?php

require_once dirname(__FILE__) . '/../fSQL.php';

abstract class fSQLBaseTest extends PHPUnit_Framework_TestCase
{
    static $tempDir = ".tmp/";

    public function setUp()
    {
        if(file_exists(self::$tempDir)) {
          self::deleteDir(self::$tempDir);
        }
        mkdir(self::$tempDir);
    }

    public static function deleteDir($dirPath) {
        if(! is_dir($dirPath)) {
            throw new InvalidArgumentException("$dirPath must be a directory");
        }
        if(substr($dirPath, strlen($dirPath) - 1, 1) != '/') {
            $dirPath .= '/';
        }
        $files = glob($dirPath . '*', GLOB_MARK);
        foreach ($files as $file) {
            if (is_dir($file)) {
                self::deleteDir($file);
            } else {
                unlink($file);
            }
        }
        rmdir($dirPath);
    }
}
