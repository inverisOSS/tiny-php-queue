<?php
namespace inverisOSS\TinyPHPQueue\tests;

class DemoTaskWorker implements \inverisOSS\TinyPHPQueue\Interfaces\ITaskWorker
{
    public function run(\inverisOSS\TinyPHPQueue\Task $task)
    {
        $metaData = $task->getMetaData();

        if (isset($metaData['simulateFailedTask'])) {
            return false;
        }

        return true;
    } // run

    public function childTasksCompleted(\inverisOSS\TinyPHPQueue\Task $task)
    {
        $metaData = $task->getMetaData();

        if (isset($metaData['simulateFailedOnChildTasksCompletedCallback'])) {
            return false;
        }

        return true;
    } // childTasksCompleted

    public function cleanup(\inverisOSS\TinyPHPQueue\Task $task)
    {
    } // cleanup
} // class DemoTaskWorker
