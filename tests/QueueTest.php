<?php
namespace inverisOSS\TinyPHPQueue\tests;

use inverisOSS\TinyPHPQueue\Config;
use inverisOSS\TinyPHPQueue\Queue;
use inverisOSS\TinyPHPQueue\Processor;
use inverisOSS\TinyPHPQueue\Task;

use inverisOSS\TinyPHPQueue\Tests\ArrayDataSet;

class QueueTest extends \PHPUnit\DbUnit\TestCase
{
    /**
     * @var PDO
     */
    private static $pdo;

    /**
     * @var PHPUnit\DbUnit\Database\DefaultConnection
     */
    private $dbcon;

    public static function setUpBeforeClass()
    {
        self::$pdo = new \PDO('sqlite::memory:');
        Config::set('db', self::$pdo);

        $tableDef = sprintf("CREATE TABLE `%s` (
            `id` INTEGER PRIMARY KEY AUTOINCREMENT,
            `parent_id` INTEGER DEFAULT NULL,
            `queue_group` VARCHAR(32) DEFAULT NULL,
            `status` VARCHAR(16),
            `processor_id` VARCHAR(32),
            `created` DATETIME,
            `first_processed` DATETIME DEFAULT NULL,
            `last_processed` DATETIME DEFAULT NULL,
            `times_executed` SMALLINT(6),
            `max_executions` SMALLINT(6) DEFAULT '0',
            `timeout` DATETIME DEFAULT NULL,
            `executed` TINYINT(1) DEFAULT '0',
            `child_failed` TINYINT(1) DEFAULT '0',
            `break_on_child_fail` TINYINT(1) DEFAULT '0',
            `task_worker_class` VARCHAR(64),
            `meta_data` MEDIUMTEXT
        );", Config::QDB_TASK_TABLE);

        self::$pdo->exec($tableDef);
    } // setUpBeforeClass

    /**
     * @return PHPUnit\DbUnit\Database\DefaultConnection
     */
    public function getConnection()
    {
        if ($this->dbcon === null) {
            if (self::$pdo == null) {
                self::$pdo = new \PDO('sqlite::memory:');
            }
            $this->dbcon = $this->createDefaultDBConnection(self::$pdo, ':memory:');
        }

        return $this->dbcon;
    } // getConnection

    /**
     * @return PHPUnit\DbUnit\DataSet\IDataSet
     */
    public function getDataSet()
    {
        return new \PHPUnit\DbUnit\DataSet\DefaultDataSet();
    } // getDataSet

    /**
     * @expectedException \InvalidArgumentException
     */
    public function testThrowsExceptionDueToInvalidProcessor()
    {
        $queue = new Queue();
        $task = $queue->getTask('InvalidProcessor');
    } // testThrowsExceptionDueToInvalidProcessor

    public function testDataOfRetrievedTaskEqualsDataOfEnqueuedTask()
    {
        $processorID = 'queueProcessor';
        $queueGroup = 'test';
        $taskData =  array(
            'queue_group' => 'test',
            'status' => Task::STATUS_PROCESSING,
            'processor_id' => $processorID,
            'timeout' => '2016-10-10 17:00:00',
            'times_executed' => 1,
            'max_executions' => 5,
            'task_worker_class' => 'TaskWorkerClassName',
            'meta_data' => json_encode(array(
                'Key 1' => 'Value 1',
                'Key 2' => 'Value 2'
            ))
        );

        $task = new Task();
        $task->populate($taskData);

        $processor = new Processor($processorID, 'test');

        $queue = new Queue();
        $queue->enqueue($task);

        $retrievedTask = $queue->getTask($processor);
        $compareTaskData = array_intersect_assoc($taskData, $retrievedTask->getPersistentData());

        $this->assertArraySubset($taskData, $retrievedTask->getPersistentData());
    } // testDataOfRetrievedTaskEqualsDataOfEnqueuedTask

    public function testCleanupDateTimeMatchesGivenTerm()
    {
        $queue = new Queue();
        $deleted = $queue->cleanup('2016-10-04');
        $this->assertEquals('2016-10-04 00:00:00', $deleted['expiry_date_time']);
    } // testCleanupDateTimeMatchesGivenTerm
} // class QueueTest
