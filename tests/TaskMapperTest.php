<?php
namespace inverisOSS\TinyPHPQueue\tests;

use inverisOSS\TinyPHPQueue\Config;
use inverisOSS\TinyPHPQueue\Task;
use inverisOSS\TinyPHPQueue\TaskMapper;

use inverisOSS\TinyPHPQueue\tests\ArrayDataSet;

class TaskMapperTest extends \PHPUnit\DbUnit\TestCase
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
    private $mapper;

    private $defaultTaskData;
    private $defaultTableColumnsToCompare;

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

        $this->mapper = new TaskMapper();
        $this->defaultTaskData =  array(
            'queue_group' => 'test',
            'status' => Task::STATUS_PROCESSING,
            'timeout' => '2016-10-10 17:00:00',
            'times_executed' => 1,
            'max_executions' => 5
        );
        $this->defaultTableColumnsToCompare = implode(', ', array_keys($this->defaultTaskData));
        ;
    } // setUp

    /**
     * @return PHPUnit\DbUnit\Database\DefaultConnection
     */
    protected function getConnection()
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
    protected function getDataSet()
    {
        return $this->createXMLDataSet(dirname(__FILE__) . DIRECTORY_SEPARATOR . 'task_fixture.xml');
    } // getDataSet

    public function testTaskRetrievedByIDIsTaskObjectWithExpectedData()
    {
        $taskID = 2;

        $task = $this->mapper->getByID($taskID);
        $this->assertInstanceOf('\inverisOSS\TinyPHPQueue\Task', $task);

        $expectedDataSet = $this->createXmlDataSet(dirname(__FILE__) . DIRECTORY_SEPARATOR . 'task_expected_get_by_id.xml');
        $retrievedTaskData = array(
            Config::QDB_TASK_TABLE => array(
                array_merge(
                    array('id' => $taskID),
                    $task->getPersistentData()
                )
            )
        );
        $actualDataSet = new ArrayDataSet($retrievedTaskData);

        $this->assertDataSetsEqual($expectedDataSet, $actualDataSet);
    } // testTaskRetrievedByIDIsTaskObjectWithExpectedData

    public function testRetrievingTasksByMultipleArgsReturnsExpectedArray()
    {
        $queryArgs = array(
            array(
                'args' => array(
                    'queue_group' => array(
                        'compare' => '=',
                        'value' => 'import'
                    ),
                    'status' => array(
                        'compare' => 'IN',
                        'value' => 'pending,done'
                    )
                ),
                'expected_result_number' => 1
            ),
            array(
                'args' => array(
                    'queue_group' => array(
                        'compare' => '=',
                        'value' => 'import'
                    ),
                    'meta_data' => array(
                        'file' => 'test.zip'
                    )
                ),
                'expected_result_number' => 2
            ),
            array(
                'args' => array(
                    'queue_group' => array(
                        'compare' => '=',
                        'value' => 'import'
                    ),
                    'meta_data' => array(
                        'file' => array(
                            'value' => 'test.zip'
                        )
                    )
                ),
                'expected_result_number' => 2
            ),
            array(
                'args' => array(
                    'queue_group' => array(
                        'compare' => '=',
                        'value' => 'import'
                    ),
                    'meta_data' => array(
                        'file' => array(
                            'value' => 'test.zip',
                            'compare' => '='
                        )
                    )
                ),
                'expected_result_number' => 2
            )
        );

        foreach ($queryArgs as $i => $args) {
            $expectedDataSet = $this->createXmlDataSet(dirname(__FILE__) . DIRECTORY_SEPARATOR . 'task_expected_get_by_multiple_args_' . $args['expected_result_number'] . '.xml');

            $tasks = $this->mapper->getAllBy($args['args']);

            $retrievedTaskData = array();
            if (is_array($tasks) && count($tasks) > 0) {
                $retrievedTaskData[Config::QDB_TASK_TABLE] = array();
                foreach ($tasks['tasks'] as $id => $task) {
                    $retrievedTaskData[Config::QDB_TASK_TABLE][] = array_merge(
                        array('id' => $id),
                        $task->getPersistentData()
                    );
                }
            }
            $actualDataSet = new ArrayDataSet($retrievedTaskData);

            $this->assertDataSetsEqual($expectedDataSet, $actualDataSet);
        }
    } // testRetrievingTasksByMultipleArgsReturnsExpectedArray

    public function testRetrievingPaginatedNumberOfTasksResultsInExpectedIDsAndPages()
    {
        $queryArgs = array(
            array(
                'args' => array(
                    'queue_group' => 'default',
                    'status' => 'pending'
                ),
                'pagination' => array(),
                'expected_ids' => array(1, 2, 3, 8),
                'expected_pages' => 1,
                'expected_page' => 1
            ),
            array(
                'args' => array(
                    'queue_group' => 'default',
                    'status' => 'pending'
                ),
                'pagination' => array(
                    'tasks_per_page' => 2
                ),
                'expected_ids' => array(1, 2),
                'expected_pages' => 2,
                'expected_page' => 1
            ),
            array(
                'args' => array(
                    'queue_group' => 'default',
                    'status' => 'pending'
                ),
                'pagination' => array(
                    'tasks_per_page' => 2,
                    'page' => 2
                ),
                'expected_ids' => array(3, 8),
                'expected_pages' => 2,
                'expected_page' => 2
            )
        );

        foreach ($queryArgs as $args) {
            $tasks = $this->mapper->getAllBy($args['args'], $args['pagination']);

            $this->assertEquals($args['expected_ids'], array_keys($tasks['tasks']));
            $this->assertEquals($args['expected_pages'], $tasks['pagination']['pages']);
            $this->assertEquals($args['expected_page'], $tasks['pagination']['page']);
        }
    } // testRetrievingPaginatedNumberOfTasksResultsInExpectedIDsAndPages

    public function testIDsOfRetrievedNextProcessableTasksMatchGivenIDs()
    {
        $expectedTasks = array(
            array(
                'processor_id' => 'noname',
                'queue_group' => 'import',
                'task_id' => 9
            ),
            array(
                'processor_id' => 'demo_processor',
                'queue_group' => 'import',
                'task_id' => 11
            )
        );

        foreach ($expectedTasks as $taskData) {
            $task = $this->mapper->getNextProcessableTask($taskData['processor_id'], $taskData['queue_group']);
            $this->assertEquals($taskData['task_id'], $task->getID());
        }
    } // testIDsOfRetrievedNextProcessableTasksMatchGivenIDs

    public function testIDsOfRetrievedChildTasksMatchGivenIDs()
    {
        $mainTaskID = 3;
        $childTask1ID = 5;
        $childTask2ID = 6;

        $mainTask = $this->mapper->getByID($mainTaskID);
        $childTask1 = $this->mapper->getNextChild($mainTask);

        $this->assertInstanceOf('\inverisOSS\TinyPHPQueue\Task', $childTask1);
        $this->assertEquals($childTask1ID, $childTask1->getID());

        $childTask1->setStatus(Task::STATUS_DONE);
        $this->mapper->save($childTask1);
        $childTask2 = $this->mapper->getNextChild($mainTask);

        $this->assertInstanceOf('\inverisOSS\TinyPHPQueue\Task', $childTask2);
        $this->assertEquals($childTask2ID, $childTask2->getID());
    } // testIDsOfRetrievedChildTasksMatchGivenIDs

    public function testDBTableContentsEqualExpectedDataAfterNewTaskRecordIsInserted()
    {
        $taskWorkerClassName = 'TaskWorkerClassName';

        $task = new Task($taskWorkerClassName);
        $task->populate($this->defaultTaskData);

        $taskID = $this->mapper->save($task);

        $expectedDataSet = new ArrayDataSet(array(Config::QDB_TASK_TABLE => array($this->defaultTaskData)));
        $actualTable = $this->getConnection()->createQueryTable(
            Config::QDB_TASK_TABLE,
            sprintf('SELECT %s FROM %s WHERE id=%s', $this->defaultTableColumnsToCompare, Config::QDB_TASK_TABLE, $taskID)
        );

        $this->assertTablesEqual($expectedDataSet->getTable(Config::QDB_TASK_TABLE), $actualTable);
    } // testDBTableContentsEqualExpectedDataAfterNewTaskRecordIsInserted

    public function testDBTableContentsEqualExpectedDataAfterTaskRecordIsUpdated()
    {
        $taskID = 2;

        $task = $this->mapper->getByID($taskID);
        $task->populate($this->defaultTaskData);

        $this->mapper->save($task);

        $expectedDataSet = new ArrayDataSet(array(Config::QDB_TASK_TABLE => array($this->defaultTaskData)));
        $actualTable = $this->getConnection()->createQueryTable(
            Config::QDB_TASK_TABLE,
            sprintf('SELECT %s FROM %s WHERE id=%s', $this->defaultTableColumnsToCompare, Config::QDB_TASK_TABLE, $taskID)
        );

        $this->assertTablesEqual($expectedDataSet->getTable(Config::QDB_TASK_TABLE), $actualTable);
    } // testDBTableContentsEqualExpectedDataAfterTaskRecordIsUpdated

    public function testStatusOfParentTasksIsUpdatedWhenChildTaskFails()
    {
        $mainTaskID = 3;
        $childTaskID = 7;

        $childTask = $this->mapper->getByID($childTaskID);
        $childTask->setStatus(Task::STATUS_FAILED);
        $this->mapper->save($childTask);

        $mainTask = $this->mapper->getByID($mainTaskID);

        $this->assertEquals(Task::STATUS_FAILED, $mainTask->getStatus());
    } // testStatusOfParentTasksIsUpdatedWhenChildTaskFails

    public function testNextProcessableChildIdEqualsGivenId()
    {
        $mainTaskID = 15;
        $expectedChildID = 17;

        $mainTask = $this->mapper->getByID($mainTaskID);
        $childTask = $this->mapper->getNextChild($mainTask);

        $this->assertEquals($expectedChildID, $childTask->getID());
    } // testNextProcessableChildIdEqualsGivenId

    public function testNextArbitraryChildIdEqualsGivenId()
    {
        $mainTaskID = 15;
        $expectedChildID = 16;

        $mainTask = $this->mapper->getByID($mainTaskID);
        $childTask = $this->mapper->getNextChild($mainTask, false);

        $this->assertEquals($expectedChildID, $childTask->getID());
    } // testNextArbitraryChildIdEqualsGivenId

    public function testNumberOfDeletedExpiredTasksMatchesGivenNumber()
    {
        $deleted = $this->mapper->deleteExpiredTasks('2016-10-04');
        $this->assertEquals(2, $deleted['deleted_count']);
    } // testNumberOfDeletedExpiredTasksMatchesGivenNumber

} // class TaskMapperTest
