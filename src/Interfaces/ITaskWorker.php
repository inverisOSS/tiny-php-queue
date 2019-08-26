<?php
namespace inverisOSS\TinyPHPQueue\Interfaces;

interface ITaskWorker
{
    public function run(\inverisOSS\TinyPHPQueue\Task $task);
    public function childTasksCompleted(\inverisOSS\TinyPHPQueue\Task $task);
    public function cleanup(\inverisOSS\TinyPHPQueue\Task $task);
} // interface ITaskWorker
