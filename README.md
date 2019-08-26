# Tiny PHP Queue

A (really) lightweight PHP/PDO based **hierarchical** queue manager

## Documentation

### Task Table Structure

id | parent_id | queue_group | status | processor_id | created | first_processed | last_processed | times_executed | max_executions | timeout | executed | child_failed | break_on_child_fail | task_worker_class | meta_data
-- | --------- | ----------- | ------ | ------------ | ------- | --------------- | -------------- | -------------- | -------------- | ------- | -------- | ------------ | ------------------- | ----------------- | ---------
1 | null | default | processing | demo_processor | 2015-10-14 16:00:00 | null | null | 0 | 10 | 2015-10-15 16:00:00 | 0 | 0 | 0 | \myProject\TaskWorkers\DemoTaskWorker | {}
2 | null | import | processing | import_processor | 2015-10-14 16:10:00 | 2015-10-14 16:12:00 | null | 1 | 0 | 2015-10-15 16:10:00 | 1 | 0 | 0 | \myProject\TaskWorkers\ImportTaskWorker | {}
3 | 2 | import | done | import_processor | 2015-10-14 16:11:00 | 2015-10-14 16:12:00 | 2015-10-14 16:12:00 | 1 | 10 | 2015-10-15 16:11:00 | 1 | 0 | 0 | \myProject\TaskWorkers\ImportChildTaskWorker | {"meta_1":"foo","meta_2":["foo","Bar"]}
4 | 2 | import | pending | | 2015-10-14 16:11:30 | null | null | 0 | 10 | 2015-10-15 16:11:30 | 0 | 0 | 0 | \myProject\TaskWorkers\ImportChildTaskWorker | {}
5 | null | import | pending | | 2015-10-14 16:20:00 | null | null | 0 | 10 | 2015-10-15 16:20:00 | 0 | 0 | 0 | \myProject\TaskWorkers\ImportTaskWorker | {}


Field | Description
----- | -----------
id | task ID (auto increment)
parent_id | parent task ID (null on top level tasks)
queue_group | arbitrary name for grouping tasks (optional)
status | task status (pending, processing, done, failed)
processor_id | name of the "processor" that performes/performed this task and all of its child tasks (if any)
created | date/time the task has been created
first_processed | date/time the task has been processed for the first time
last_processed | date/time the task has been processed for the last time
times_executed | number of executions
max_executions | maximum number of executions (0 = unlimited)
timeout | execution timeout (null = unlimited)
executed | 1 if the execution of the main task code has been completed (independent of eventually still pending child tasks)
child_failed | 1 if the execution of ONE of the task's child tasks have failed
break_on_child_fail | 1 if the task execution should fail if a single child task fails.
task_worker_class | "worker class" that contains the task execution code (including namespace)
meta_data | arbitrary meta data as JSON string (available/extendable via task and child task worker code)

### Installation

#### Composer

composer.json:

```json
    "repositories": [
        {
            "type": "vcs",
            "url": "https://github.com/inverisOSS/tiny-php-queue"
        }
    ],
    "require": {
        "inverisoss/tiny-php-queue": "*"
    }
```

`php composer.phar install`

### Initialization

```php
$pdo = new \PDO(
    'mysql:host=127.0.0.1;dbname=queue;charset=utf8',
    'queuedb_username',
    'queuedb_password',
    array(
        \PDO::ATTR_EMULATE_PREPARES => false,
        \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
    )
));

\inverisoss\TinyPHPQueue\Config::set('db', $pdo);
```

### Task Worker Implementation

Every type of task has its own worker class. Worker classes have to implement the ITaskWorker interface with the following mandatory methods:

**run**: the main task code (It's possible to define child tasks here.)

**childTasksCompleted**: code that shall be executed when the execution of all child tasks has finished

**cleanup**: code to run after the task has been processed (no matter if successfully or not)

#### Example

```php
use inverisOSS\TinyPHPQueue\Task;
use inverisOSS\TinyPHPQueue\Queue;

class ImportTaskWorker implements \inverisoss\TinyPHPQueue\Interfaces\ITaskWorker
{
    public function run(Task $task)
    {
        // The main task code... Examples:

        // Retrieve the current task meta data.
        $metaData = $task->getMetaData();

        // Update task meta data.
        $task->updateMetaData('processingStart', date('Y-m-d H:i:s'), true);

        /**
         * Add child tasks.
         */

        $xmlFiles = glob('/path/to/files/*.xml');

        if (count($xmlFiles) > 0) {
            $queue = new Queue();

            foreach ($xmlFiles as $xmlFile) {
                // Add a child task.
                $childTaskMeta = array(
                    'xmlFile' => $xmlFile
                );

                $childTask = new Task('\myProject\TaskWorkers\ProcessImportXmlFile', 'import', $childTaskMeta);
                $childTask->setParent($task);
                $childTask->setMaxExecutions(10);
                $queue->enqueue($childTask);
            }
        }

        return true;
    } // run

    public function childTasksCompleted(Task $task)
    {
        // Stuff to execute when all child tasks have been completed...

        // Always return true here.
        return true;
    } // childTasksCompleted

    public function cleanup(Task $task)
    {
        // "Cleanup" code...
    } // cleanup
} // class ImportTaskWorker
```

### Queue Processing

#### Basic Example (Invocation via HTTPS-Request)

```php
use inverisOSS\TinyPHPQueue\Queue;
use inverisOSS\TinyPHPQueue\Processor;

$importGroup = 'import';
$lockDir = '.lock';
mkdir($lockDir);

$queue = new Queue();
$processor = new Processor('import_processor_1', $importGroup);

$cycles = 0;

while ($task = $queue->getTask($processor)) {
    $processor->execute($task);
    $cycles++;

    if ($cycles > 20) {
        // Max. number of queue cycles reached - reload the page to proceed.
        $location = 'Location: https://' . $_SERVER[HTTP_HOST] . $_SERVER[REQUEST_URI];
        rmdir($lockDir);
        header($location);
        die();
    }
}
```

### Cleanup

```php
use inverisOSS\TinyPHPQueue\Queue;

$queue = new Queue();

// Delete all tasks (including child tasks) older than 6 months.
$queue->cleanup('6 months ago');
```

## License

Apache License 2.0
