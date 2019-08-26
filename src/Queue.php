<?php
namespace inverisOSS\TinyPHPQueue;

use inverisOSS\TinyPHPQueue\TaskMapper;

use inverisOSS\TinyPHPQueue\Exceptions\ConfigException;

class Queue
{
    private $taskMapper;

    public function __construct()
    {
        $this->taskMapper = new TaskMapper();
    } // __construct

    /**
     * Enqueue a task.
     *
     * @since 0.1
     *
     * @param Task $task Task object to add to queue.
     */
    public function enqueue($task)
    {
        $this->taskMapper->save($task);
    } // enqueue

    /**
     * Return the next task to be processed by the given processor.
     *
     * @since 0.1
     *
     * @param Processor $processor Processor object.
     *
     * @return Task|bool Next processable task or false if nonexistent.
     */
    public function getTask($processor)
    {
        if (!$processor instanceof Processor) {
            throw new \InvalidArgumentException('No valid Processor object passed.');
        }

        return $this->taskMapper->getNextProcessableTask($processor->getID(), $processor->getQueueGroup());
    } // getTask

    public function cleanup($expiredBefore)
    {
        return $this->taskMapper->deleteExpiredTasks($expiredBefore);
    } // cleanup
} // class Queue
