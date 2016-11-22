<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\File;

class FileTest extends BaseTest {

    public function test__destruct() {
        $file = new File("garbage/path");
        $file->__destruct();
    }

    public function testGetHandleClosed() {
        $file = new File("garbage/path");
        $this->assertNull($file->GetHandle());
    }

    public function testGetHandleOpen() {
        $file = new File(parent::$tempDir . "something.txt");
        $file->open('w+');
        $this->assertNotNull($file->GetHandle());
    }

    public function testGetPath() {
        $filePath = "garbage/path";
        $file = new File($filePath);
        $this->assertEquals($filePath, $file->getPath());
    }

    public function testExists() {
        $filePath = parent::$tempDir . "something.txt";
        $file = new File($filePath);
        $this->assertFalse($file->exists());
        $file->open('w+');
        $this->assertTrue($file->exists());
        $file->close();
    }

    public function testOpen() {
        $filePath = parent::$tempDir . "something.txt";
        $file = new File($filePath);
        $this->assertFalse($file->open('r'));
        $this->assertTrue($file->open('w+'));
    }

    public function testLock() {
        $file = new File(parent::$tempDir . "something.txt");
        $file->open('w+');
        $this->assertTrue($file->lock(LOCK_SH));
    }

    public function testClose() {
        $file = new File(parent::$tempDir . "something.txt");
        $file->open('w+');
        $this->assertTrue($file->exists());

        $file->close();
        $this->assertNull($file->getHandle());
        $this->assertTrue($file->exists());
    }

    public function testDropClosed() {
        touch(parent::$tempDir . "something.txt");
        $file = new File(parent::$tempDir . "something.txt");
        $this->assertTrue($file->exists());

        $file->drop();
        $this->assertFalse($file->exists());
    }

    public function testDropOpen() {
        $file = new File(parent::$tempDir . "something.txt");
        $file->open('w+');
        $this->assertNotNull($file->getHandle());
        $this->assertTrue($file->exists());

        $file->drop();
        $this->assertTrue($file->exists());
    }
}
