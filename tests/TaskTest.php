<?php
namespace inverisOSS\TinyPHPQueue\tests;

use inverisOSS\TinyPHPQueue\Config;
use inverisOSS\TinyPHPQueue\Task;

class TaskTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @var Task
     */
    private $task;

    public function setUp()
    {
        $this->defaultTaskWorkerClassName = 'TaskWorkerClassName';

        $this->task = new Task();
    } // setUp

    public function testTaskGetsCreatedWithCorrectInitialValues()
    {
        $taskWorkerClassName = 'TaskWorkerClassName';

        $task = new Task($taskWorkerClassName);

        $this->assertAttributeEquals(Config::DEFAULT_TASK_STATUS, 'status', $task);
        $this->assertAttributeEquals(Config::DEFAULT_QUEUE_GROUP, 'queueGroup', $task);
        $this->assertEquals($taskWorkerClassName, $task->getTaskWorkerClass());
        $this->assertNull($task->getID());
    } // testTaskGetsCreatedWithCorrectInitialValues

    public function testParentEqualsAddedParentObject()
    {
        $parentTaskID = 1;

        $parent = new Task($this->defaultTaskWorkerClassName);
        $parent->setID($parentTaskID);
        $child = new Task($this->defaultTaskWorkerClassName);

        $parent->addChild($child);

        $this->assertAttributeEquals($parent, 'parent', $child);
    } // testParentEqualsAddedParentObject

    /**
     * @expectedException \UnexpectedValueException
     */
    public function testThrowExceptionDueToMissingParentTaskID()
    {
        $parent = new Task($this->defaultTaskWorkerClassName);
        $child = new Task($this->defaultTaskWorkerClassName);

        $parent->addChild($child);
    } // testThrowExceptionDueToMissingParentTaskID

    public function testLastProcessedTimeEqualsCurrentTime()
    {
        $this->task->updateLastProcessed();
        $currentTime = time();
        $lastProcessed = $this->task->getLastProcessed();

        $this->assertNotFalse($lastProcessed);

        $lastProcessedTs = strtotime($lastProcessed);

        $isCurrentTime = $lastProcessedTs >= $currentTime - 5 && $lastProcessedTs <= $currentTime;

        $this->assertTrue($isCurrentTime);
    } // testLastProcessedTimeEqualsCurrentTime

    public function testTimesExecutedEqualsGivenValue()
    {
        $executionCount = 3;

        for ($i = 0; $i < $executionCount; $i++) {
            $this->task->incrementTimesExecuted();
        }

        $this->assertEquals($executionCount, $this->task->getTimesExecuted());
    } // testTimesExecutedEqualsGivenValue

    public function testMaxExecutionsCountEqualsGivenValue()
    {
        $maxExecutions = 5;

        $this->task->setMaxExecutions($maxExecutions);

        $this->assertEquals($maxExecutions, $this->task->getMaxExecutions());
    } // testMaxExecutionsCountEqualsGivenValue

    public function testTimeoutEqualsGivenValue()
    {
        $timeout = '2016-10-01 19:00:00';

        $this->task->setTimeout($timeout);

        $this->assertEquals($timeout, $this->task->getTimeout());
    } // testMaxExecutionsCountEqualsGivenValue

    public function testExecutedFlagIsSet()
    {
        $this->task->setExecuted(true);

        $this->assertTrue($this->task->getExecuted());
    } // testtestExecutedFlagIsSet

    public function testObjectMetaDataEqualSetMetaData()
    {
        $parentTaskID = 1;
        $parentMetaData = array(
            'Parent Key 1' => 'Value 1',
            'Parent Key 2' => 'Value 2'
        );
        $childMetaData = array(
            'Child Key 1' => 'Value 1',
            'Child Key 2' => 'Value 2'
        );

        $parent = new Task($this->defaultTaskWorkerClassName);
        $parent->setID($parentTaskID);
        $child = new Task($this->defaultTaskWorkerClassName);

        $parent->addChild($child);

        foreach ($parentMetaData as $key => $value) {
            $child->updateMetaData($key, $value, true); // global flag set = add data to parent object
        }

        foreach ($childMetaData as $key => $value) {
            $child->updateMetaData($key, $value, false);
        }

        $this->assertEquals($parentMetaData, $parent->getMetaData());
        $this->assertEquals(array_merge($parentMetaData, $childMetaData), $child->getMetaData());
    } // testMaxExecutionsCountEqualsGivenValue

    public function testTaskGetsFailedStatusDueToChildFailure()
    {
        $parentTaskID = 1;

        $parent = new Task($this->defaultTaskWorkerClassName);
        $parent->setID($parentTaskID);
        $parent->setBreakOnChildFail(true);
        $child = new Task($this->defaultTaskWorkerClassName);

        $parent->addChild($child);
        $child->setStatus(Task::STATUS_FAILED);

        $this->assertEquals(Task::STATUS_FAILED, $parent->getStatus());
        $this->assertTrue($parent->getChildFailed());
    } // testTaskGetsFailedStatusDueToChildFailure

    public function testIdsOfDeletedTasksMatchesGivenArray()
    {
        $parentTaskID = 15;
        $expectedDeletedIDs = [15, 16, 17, 18, 19];

        $parent = new Task($this->defaultTaskWorkerClassName);
        $parent->setID($parentTaskID);

        $this->assertEquals($expectedDeletedIDs, $parent->delete());
    } // testIdsOfDeletedTasksMatchesGivenArray
} // class TaskTest
