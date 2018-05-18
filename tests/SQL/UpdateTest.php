<?php

require_once dirname(__DIR__).'/BaseTest.php';

use FSQL\Database\CachedTable;
use FSQL\Environment;
use FSQL\ResultSet;

class UpdateTest extends BaseTest
{
    private $fsql;

    private static $options = [
        'field_name' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
        'field_data' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $optionsWithKey = [
        'field_name' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'p', 'null' => 1, 'restraint' => []],
        'field_data' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'n', 'null' => 1, 'restraint' => []],
    ];

    private static $optionsWithMultiKey = [
        'field_name' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'p', 'null' => 1, 'restraint' => []],
        'field_data' => ['type' => 's', 'auto' => 0, 'default' => null, 'key' => 'p', 'null' => 1, 'restraint' => []],
    ];

    public function setUp()
    {
        parent::setUp();
        $this->fsql = new Environment();
        $this->fsql->define_db('db1', parent::$tempDir);
        $this->fsql->select_db('db1');
    }

    public function testNoWhere()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$options);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE options SET field_data = '99'" );
        $this->assertTrue($result);

        $expected = [
            ['graph_data_amount_total', '99'],
            ['graph_data_range_recv', '99'],
            ['graph_data_range_sent', '99'],
            ['max_active_peers', '99'],
        ];
        $this->assertEquals($expected, $table->getEntries());
        $this->assertEquals(4, $this->fsql->affected_rows());
    }

    public function testEmptyTable()
    {
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$options);
        $result = $this->fsql->query("UPDATE options SET field_data = '---time=1234567890---total=99'" );
        $this->assertTrue($result);

        $this->assertEquals([], $table->getEntries());
        $this->assertEquals(0, $this->fsql->affected_rows());
    }

    public function testNoMatch()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$options);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE options SET field_data = '---time=1234567890---total=99' WHERE field_name = 'username'" );
        $this->assertTrue($result);

        $this->assertEquals($rows, $table->getEntries());
        $this->assertEquals(0, $this->fsql->affected_rows());
    }

    public function testSetClauseEmbeddedEqual()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$options);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE options SET field_data = '---time=1234567890---total=99' WHERE field_name = 'graph_data_amount_total'" );
        $this->assertTrue($result);

        $expected = [
            ['graph_data_amount_total', '---time=1234567890---total=99'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $this->assertEquals($expected, $table->getEntries());
        $this->assertEquals(1, $this->fsql->affected_rows());
    }

    public function testKeyNoCollision()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$optionsWithKey);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE options SET field_name = 'max_total_peers' WHERE field_name = 'graph_data_range_recv'" );
        var_dump($this->fsql->error());
        $this->assertTrue($result);

        $expected = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['max_total_peers', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];

        $this->assertEquals($expected, $table->getEntries());
        $this->assertEquals(1, $this->fsql->affected_rows());
    }

    public function testCollisionIgnore()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$optionsWithKey);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE IGNORE options SET field_name = 'max_active_peers' WHERE field_name = 'graph_data_range_recv'" );
        $this->assertTrue($result);

        $this->assertEquals($rows, $table->getEntries());
        $this->assertEquals(0, $this->fsql->affected_rows());
    }

    public function testCollision()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=15'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$optionsWithKey);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE options SET field_name = 'max_active_peers' WHERE field_name = 'graph_data_range_recv'" );
        $this->assertFalse($result);

        $this->assertEquals($rows, $table->getEntries());
        $this->assertEquals(0, $this->fsql->affected_rows());
    }

    public function testCollisionMultiKeys()
    {
        $rows = [
            ['graph_data_amount_total', '---time=1234567890---total=15'],
            ['graph_data_range_recv', '---time=1234567890---total=5'],
            ['graph_data_range_sent', '---time=1234567890---total=12'],
            ['max_active_peers', '5'],
        ];
        $table = CachedTable::create($this->fsql->current_schema(), 'options', self::$optionsWithMultiKey);
        $cursor = $table->getWriteCursor();
        foreach($rows as $row) {
            $cursor->appendRow($row);
        }
        $table->commit();
        $result = $this->fsql->query("UPDATE options SET field_name = 'max_active_peers', field_data = '5' WHERE field_name = 'graph_data_range_recv'" );
        $this->assertFalse($result);

        $this->assertEquals($rows, $table->getEntries());
        $this->assertEquals(0, $this->fsql->affected_rows());
    }
}
