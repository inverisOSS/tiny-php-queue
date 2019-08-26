<?php
namespace inverisOSS\TinyPHPQueue;

use inverisOSS\TinyPHPQueue\Task;
use inverisOSS\TinyPHPQueue\TaskMapper;

class Processor
{
    private $ID;
    private $queueGroup;

    /**
     * The constructor...
     *
     * @since 0.1
     *
     * @param string $id Unique processor ID.
     * @param string $queueGroup Group name of tasks to process (optional).
     */
    public function __construct($id, $queueGroup = false)
    {
        $this->ID = $id;
        $this->queueGroup = $queueGroup ? $queueGroup : Config::DEFAULT_QUEUE_GROUP;
    } // __construct

    /**
     * Return the unique processor ID.
     *
     * @since 0.1
     *
     * @return string Processor ID.
     */
    public function getID()
    {
        return $this->ID;
    } // getID

    /**
     * Return the assigned queue group.
     *
     * @since 0.1
     *
     * @return string Queue group.
     */
    public function getQueueGroup()
    {
        return $this->queueGroup;
    } // getQueueGroup

    /**
     * Execute a task.
     *
     * @since 0.1
     *
     * @return Task $task Task object.
     */
    public function execute($task)
    {
        $taskMapper = new TaskMapper();

        $task->setProcessorID($this->getID());
        $task->setStatus(Task::STATUS_PROCESSING);
        $task->updateLastProcessed();
        $taskMapper->save($task);

        if (class_exists($task->getTaskWorkerClass())) {
            $taskWorkerClass = $task->getTaskWorkerClass();
            $taskWorker = new $taskWorkerClass;
        } else {
            $task->setStatus(Task::STATUS_FAILED);
            $taskMapper->save($task);
            throw new \RuntimeException(sprintf('TaskWorker class not found: %s', $task->getTaskWorkerClass()));
        }

        if ($task->getTimeout() && $task->getLastProcessed() >= $task->getTimeout()) {
            // Task has exceeded its timeout: Set failed status and return.
            $task->setStatus(Task::STATUS_FAILED);
            $taskMapper->save($task);
            $taskWorker->cleanup($task);
            $taskMapper->save($task);
            return;
        }

        if (! $task->getExecuted()) {
            $task->incrementTimesExecuted();
            $taskMapper->save($task);

            if (
                $task->getTimesExecuted() <= $task->getMaxExecutions() ||
                0 == $task->getMaxExecutions()
            ) {
                $success = $taskWorker->run($task);
                if ($success) {
                    $task->setExecuted(true);
                }
                $taskMapper->save($task);

                if (in_array($task->getStatus(), array(Task::STATUS_DONE, Task::STATUS_FAILED))) {
                    // Task status (done or failed) has been set in worker: Invoke cleanup callback.
                    $taskWorker->cleanup($task);
                    $taskMapper->save($task);
                }
            } else {
                $task->setStatus(Task::STATUS_FAILED);
                $taskMapper->save($task);
                $taskWorker->cleanup($task);
                $taskMapper->save($task);
            }
        } else {
            $nextChildTask = $taskMapper->getNextChild($task);

            if ($nextChildTask) {
                $this->execute($nextChildTask);
            } else {
                $childTasksCompletedSuccess = $taskWorker->childTasksCompleted($task);

                $task->setStatus($childTasksCompletedSuccess ? Task::STATUS_DONE : Task::STATUS_FAILED);
                $taskMapper->save($task);
                $taskWorker->cleanup($task);
                $taskMapper->save($task);
            }
        }
    } // execute
} // class Processor
