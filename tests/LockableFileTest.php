<?php

require_once __DIR__.'/BaseTest.php';

use FSQL\File;
use FSQL\LockableFile;

class LockableFileTest extends BaseTest {

    public function test__destruct() {
        $file = new File("garbage/path");
        $lockable = new LockableFile($file);
        $lockable->__destruct();
    }

    public function testFile() {
        $file = new File(parent::$tempDir . "garbage.txt");
        $lockable = new LockableFile($file);
        $this->assertEquals($file, $lockable->file());
    }

    public function testGetHandleOpen() {
        $file = new File(parent::$tempDir . "garbage.txt");
        $file->open('w+');

        $lockable = new LockableFile($file);
        $this->assertEquals($file->getHandle(), $lockable->file()->getHandle());
    }

    public function testGetHandleClosed() {
        $file = new File(parent::$tempDir . "garbage.txt");
        $lockable = new LockableFile($file);
        $this->assertNull($lockable->getHandle());
    }

    public function testGetPath() {
        $file = new File(parent::$tempDir . "garbage.txt");
        $lockable = new LockableFile($file);
        $this->assertEquals($file->getPath(), $lockable->getPath());
    }

    public function testExists() {
        $file = new File(parent::$tempDir . "garbage.txt");

        $lockable = new LockableFile($file);
        $this->assertFalse($lockable->exists());

        $file->open('w+');
        $this->assertTrue($lockable->exists());
    }

    public function testDropOpen() {
        $file = new File(parent::$tempDir . "garbage.txt");

        $lockable = new LockableFile($file);
        $file->open('w+');

        $lockable->drop();
        $this->assertTrue($lockable->exists());
    }

    public function testDropClosed() {
        touch(parent::$tempDir . "garbage.txt");
        $file = new File(parent::$tempDir . "garbage.txt");
        $lockable = new LockableFile($file);
        $this->assertTrue($lockable->exists());

        $lockable->drop();
        $this->assertFalse($lockable->exists());
    }

    public function testAcquireReadNoFile() {
        $file = new File(parent::$tempDir . "blah.txt");
        $lockable = new LockableFile($file);
        $this->assertFalse($lockable->acquireRead());
    }

    public function testReleaseReadNoLock() {
        $file = new File(parent::$tempDir . "blah.txt");
        $lockable = new LockableFile($file);
        $this->assertTrue($lockable->releaseRead());
    }

    public function tesReadLockNewLock() {
        $file = new File(parent::$tempDir . "garbage.txt");
        touch($file->getPath());
        $lockable = new LockableFile($file);
        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());

        $this->assertTrue($lockable->acquireRead());
        $this->assertNotNull($lockable->getHandle());
        $this->assertEquals(1, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());

        $this->assertTrue($lockable->releaseRead());
        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());
    }

    public function testAcquireReadRecurse() {
        $file = new File(parent::$tempDir . "garbage.txt");
        touch($file->getPath());
        $lockable = new LockableFile($file);
        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());

        $this->assertTrue($lockable->acquireRead());
        $this->assertTrue($lockable->acquireRead());
        $this->assertTrue($lockable->acquireRead());
        $this->assertTrue($lockable->acquireRead());

        $this->assertEquals(4, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());

        $this->assertTrue($lockable->releaseRead());
        $this->assertTrue($lockable->releaseRead());
        $this->assertTrue($lockable->releaseRead());
        $this->assertTrue($lockable->releaseRead());

        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());
    }

    public function testAcquireWriteIsDir() {
        $file = new File(parent::$tempDir);
        $lockable = new LockableFile($file);
        $this->assertFalse($lockable->acquireWrite());
    }

     public function testWriteLockNewLock() {
        $file = new File(parent::$tempDir . "garbage.txt");
        $lockable = new LockableFile($file);
        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());

        $this->assertTrue($lockable->acquireWrite());
        $this->assertTrue($lockable->acquireWrite());
        $this->assertNotNull($lockable->getHandle());
        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(2, $lockable->writerCount());

        $this->assertTrue($lockable->releaseWrite());
        $this->assertTrue($lockable->releaseWrite());
        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());
    }

    public function testWriteLockWithReaders() {
        $file = new File(parent::$tempDir . "garbage.txt");
        $lockable = new LockableFile($file);

        $this->assertTrue($lockable->acquireWrite());
        $this->assertTrue($lockable->acquireWrite());

        $this->assertTrue($lockable->acquireRead());
        $this->assertTrue($lockable->acquireRead());

        $this->assertEquals(2, $lockable->readerCount());
        $this->assertEquals(2, $lockable->writerCount());

        $this->assertTrue($lockable->releaseRead());
        $this->assertTrue($lockable->releaseRead());

        $this->assertTrue($lockable->releaseWrite());
        $this->assertTrue($lockable->releaseWrite());
        $this->assertTrue($lockable->releaseWrite());

        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());
    }

    public function testReadLockUpgrade() {
        $file = new File(parent::$tempDir . "garbage.txt");
        touch($file->getPath());
        $lockable = new LockableFile($file);

        $this->assertTrue($lockable->acquireRead());
        $this->assertTrue($lockable->acquireRead());

        $this->assertTrue($lockable->acquireWrite());
        $this->assertTrue($lockable->acquireWrite());
        $this->assertTrue($lockable->acquireWrite());

        $this->assertEquals(2, $lockable->readerCount());
        $this->assertEquals(3, $lockable->writerCount());

        $this->assertTrue($lockable->releaseWrite());
        $this->assertTrue($lockable->releaseWrite());
        $this->assertTrue($lockable->releaseWrite());
        $this->assertTrue($lockable->releaseRead());
        $this->assertTrue($lockable->releaseRead());

        $this->assertEquals(0, $lockable->readerCount());
        $this->assertEquals(0, $lockable->writerCount());
    }
}
