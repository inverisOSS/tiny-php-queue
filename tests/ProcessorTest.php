<?php
namespace inverisOSS\TinyPHPQueue\tests;

require_once(dirname(__FILE__) . '/DemoTaskWorker.php');

use inverisOSS\TinyPHPQueue\Config;
use inverisOSS\TinyPHPQueue\Queue;
use inverisOSS\TinyPHPQueue\Processor;
use inverisOSS\TinyPHPQueue\Task;
use inverisOSS\TinyPHPQueue\TaskMapper;
use inverisOSS\TinyPHPQueue\tests\DemoTaskWorker;

class ProcessorTest extends \PHPUnit\DbUnit\TestCase
{
    /**
     * @var PDO
     */
    private static $pdo;

    /**
     * @var PHPUnit\DbUnit\Database\DefaultConnection
     */
    private $dbcon;

    /**
     * @var TaskMapper
     */
    private $taskMapper;

    /**
     * @var Queue
     */
    private $queue;

    /**
     * @var Processor
     */
    private $processor;

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

    protected function setUp(): void
    {
        parent::setUp();

        $processorID = 'proc';
        $queueGroup = 'default';

        $this->taskMapper = new TaskMapper();
        $this->queue = new Queue();
        $this->processor = new Processor($processorID, $queueGroup);
    } // setUp

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
    }

    /**
     * @return PHPUnit\DbUnit\DataSet\IDataSet
     */
    public function getDataSet()
    {
        return $this->createXMLDataSet(dirname(__FILE__) . DIRECTORY_SEPARATOR . 'task_fixture.xml');
    }

    public function testExecutedTaskDataEqualGivenValues()
    {
        $task = $this->queue->getTask($this->processor);
        $this->processor->execute($task);

        $this->assertEquals(substr(date('Y-m-d H:i'), 0, 16), substr($task->getLastProcessed(), 0, 16));
        $this->assertEquals(Task::STATUS_PROCESSING, $task->getStatus());
        $this->assertEquals(1, $task->getTimesExecuted());
        $this->assertTrue($task->getExecuted());
    } // testExecutedTaskDataEqualGivenValues

    public function testTaskStatusIsDoneAfterChildTasksAreCompleted()
    {
        $taskID = 3; // Task with 3 child tasks.

        $task = $this->taskMapper->getByID($taskID);

        $this->processor->execute($task);
        $this->assertEquals(Task::STATUS_PROCESSING, $task->getStatus());

        while (! in_array($task->getStatus(), array(Task::STATUS_DONE, Task::STATUS_FAILED))) {
            $this->processor->execute($task);
        }
        $this->assertEquals(Task::STATUS_DONE, $task->getStatus());
    } // testTaskStatusIsDoneAfterChildTasksAreCompleted

    public function testTaskStatusIsFailedDueToFailedChildTasksCompletedCallback()
    {
        $taskID = 3; // Task with 3 child tasks.

        $task = $this->taskMapper->getByID($taskID);
        $task->updateMetaData('simulateFailedOnChildTasksCompletedCallback', true, true);

        while (! in_array($task->getStatus(), array(Task::STATUS_DONE, Task::STATUS_FAILED))) {
            $this->processor->execute($task);
        }
        $this->assertEquals(Task::STATUS_FAILED, $task->getStatus());
    } // testTaskStatusIsFailedDueToFailedChildTasksCompletedCallback

    public function testTaskFailsDueToTimeout()
    {
        $taskID = 1;

        $task = $this->taskMapper->getByID($taskID);
        $task->setTimeout('-1 day');

        $this->processor->execute($task);
        $this->assertEquals(Task::STATUS_FAILED, $task->getStatus());
    } // testTaskFailsDueToTimeout

    public function testTaskFailsDueToExceededMaxExecutions()
    {
        $taskID = 1;
        $maxExecutions = 5;

        $task = $this->taskMapper->getByID($taskID);
        $task->setMaxExecutions($maxExecutions);
        $task->updateMetaData('simulateFailedTask', true);

        for ($i = 0; $i <= $maxExecutions; $i++) {
            $this->processor->execute($task);
        }
        $this->assertEquals(Task::STATUS_FAILED, $task->getStatus());
    } // testTaskFailsDueToExceededMaxExecutions

    /**
     * @expectedException \RuntimeException
     */
    public function testThrowsExceptionDueToNonexistentTaskWorker()
    {
        $taskID = 8; // Task with nonexistent TaskWorker class.

        $task = $this->taskMapper->getByID($taskID);
        $this->processor->execute($task);
    } // testThrowsExceptionDueToNonexistentTaskWorker
} // ProcessorTest
