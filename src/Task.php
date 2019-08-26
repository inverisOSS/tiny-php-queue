<?php
namespace inverisOSS\TinyPHPQueue;

class Task
{
    const
        STATUS_PENDING = 'pending',
        STATUS_PROCESSING = 'processing',
        STATUS_DONE = 'done',
        STATUS_FAILED = 'failed';

    private $ID;
    private $parent = false;
    private $queueGroup;
    private $status;
    private $processorID = false;
    private $created;
    private $firstProcessed;
    private $lastProcessed;
    private $timesExecuted = 0;
    private $maxExecutions = 0;
    private $timeout = false;
    private $executed = false;
    private $childFailed = false;
    private $breakOnChildFail = false;
    private $taskWorkerClass = '';
    private $metaData = array();

    public function __construct($taskWorkerClass = '', $queueGroup = false, $metaData = array())
    {
        $this->setQueueGroup($queueGroup);
        $this->setStatus(Config::DEFAULT_TASK_STATUS);
        $this->taskWorkerClass = $taskWorkerClass;
        $this->created = date('Y-m-d H:i:s');
        $this->metaData = $metaData;
    } // __construct

    /**
     * Return the task ID.
     *
     * @since 0.1
     *
     * @return int|bool ID or false if not set yet.
     */
    public function getID()
    {
        return $this->ID;
    } // getID

    /**
     * Return the parent task, if any.
     *
     * @since 0.1
     *
     * @return Task|bool Parent task or false if nonexistent.
     */
    public function getParent()
    {
        return $this->parent ? $this->parent : false;
    } // getParent

    /**
     * Return the task's queue group.
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
     * Return the task's status.
     *
     * @since 0.1
     *
     * @return string Task status.
     */
    public function getStatus()
    {
        return $this->status;
    } // getStatus

    /**
     * Return the task's processor ID.
     *
     * @since 0.1
     *
     * @return string Processor ID.
     */
    public function getProcessorID()
    {
        return $this->processorID;
    } // getProcessorID

    /**
     * Return task creation date/time.
     *
     * @since 0.1
     *
     * @return string Date/Time (YYYY-MM-DD HH:MM:SS).
     */
    public function getCreationTime()
    {
        return $this->created;
    } // getCreationTime

    /**
     * Return date/time of first processing.
     *
     * @since 0.1
     *
     * @return string|bool Date/Time (YYYY-MM-DD HH:MM:SS) or false if not set yet.
     */
    public function getFirstProcessed()
    {
        return $this->firstProcessed ? $this->firstProcessed : false;
    } // getFirstProcessed

    /**
     * Return date/time of last processing.
     *
     * @since 0.1
     *
     * @return string|bool Date/Time (YYYY-MM-DD HH:MM:SS) or false if not set yet.
     */
    public function getLastProcessed()
    {
        return $this->lastProcessed ? $this->lastProcessed : false;
    } // getLastProcessed

    /**
     * Return current number of main job executions.
     *
     * @since 0.1
     *
     * @return int Number of main job executions.
     */
    public function getTimesExecuted()
    {
        return $this->timesExecuted;
    } // getTimesExecuted

    /**
     * Return maximum number of main job executions.
     *
     * @since 0.1
     *
     * @return int Maximum number of main job executions.
     */
    public function getMaxExecutions()
    {
        return $this->maxExecutions;
    } // getMaxExecutions

    /**
     * Return the task's timeout.
     *
     * @since 0.1
     *
     * @return string|bool Timeout date/time (YYYY-MM-DD HH:MM:SS) or false if not set.
     */
    public function getTimeout()
    {
        return $this->timeout;
    } // getTimeout

    /**
     * Return main job execution status.
     *
     * @since 0.1
     *
     * @return bool Main job executed?
     */
    public function getExecuted()
    {
        return $this->executed;
    } // getExecuted

    /**
     * Return the break-on-child-fail flag.
     *
     * @since 0.1
     *
     * @return bool True if task should break on child failed child task.
     */
    public function getBreakOnChildFail()
    {
        return $this->breakOnChildFail;
    } // getBreakOnChildFail

    /**
     * Return the child-failed flag.
     *
     * @since 0.1
     *
     * @return bool True if a child task failed.
     */
    public function getChildFailed()
    {
        return $this->childFailed;
    } // getChildFailed

    /**
     * Return the name of the task's worker class.
     *
     * @since 0.1
     *
     * @return string Worker class name.
     */
    public function getTaskWorkerClass()
    {
        return $this->taskWorkerClass;
    } // getTaskWorkerClass

    /**
     * Return (possibly merged) array of meta data.
     *
     * @since 0.1
     *
     * @return array Meta data of task and its parent (if existing).
     */
    public function getMetaData()
    {
        $parentMetaData = $this->parent ? $this->parent->getMetaData() : array();
        return array_merge($parentMetaData, $this->metaData);
    } // getMetaData

    /**
     * Set the task's ID.
     *
     * @since 0.1
     *
     * @param int $id Task ID.
     */
    public function setID($id)
    {
        $this->ID = $id;
    } // setID

    /**
     * Set the parent task.
     *
     * @since 0.1
     *
     * @param Task Parent object.
     */
    public function setParent(Task $parent)
    {
        if ($parent instanceof self && $parent !== $this) {
            $this->parent = $parent;
        }
    } // setParent

    /**
     * Set the queue group.
     *
     * @since 0.1
     *
     * @param string $queueGroup Group name.
     */
    public function setQueueGroup($queueGroup)
    {
        if (! $this->parent) {
            $this->queueGroup = $queueGroup ? $queueGroup : Config::DEFAULT_QUEUE_GROUP;
        }
    } // setQueueGroup

    /**
     * Set the the task's status.
     *
     * @since 0.1
     *
     * @param string $status Status.
     */
    public function setStatus($status)
    {
        if ($this->status === $status) {
            return;
        }

        $this->status = $status;
        if ($this->status === static::STATUS_PENDING) {
            $this->setProcessorID(false);
        }

        if ($this->parent) {
            $this->parent->childStatusChanged($this);
        }
    } // setStatus

    /**
     * Set the the task's processor ID.
     *
     * @since 0.1
     *
     * @param string $processorID Processor ID.
     */
    public function setProcessorID($processorID)
    {
        $this->processorID = $processorID;
    } // setProcessorID

    /**
     * Set main job execution status.
     *
     * @since 0.1
     *
     * @param bool|string $executed Main job executed?
     */
    public function setExecuted($executed)
    {
        $this->executed = $executed ? true : false;
    } // setExecuted

    /**
     * Set maximum number of main job executions.
     *
     * @since 0.1
     *
     * @param int $max Maximum number of main job executions.
     */
    public function setMaxExecutions($max)
    {
        $this->maxExecutions = $max;
    } // setMaxExecutions

    /**
     * Set the task's timeout.
     *
     * @since 0.1
     *
     * @param $timeout string|bool Timeout date/time (processable by strtotime) or false to disable.
     */
    public function setTimeout($timeout)
    {
        if (! $timeout) {
            $this->timeout = false;
            return;
        }

        $timeoutTs = strtotime($timeout);
        if ($timeoutTs) {
            $this->timeout = date('Y-m-d H:i:s', $timeoutTs);
        }
    } // setTimeout

    /**
     * Set break behaviour if a child task fails.
     *
     * @since 0.1
     *
     * @param boolean $break Break if a child task failed?
     */
    public function setBreakOnChildFail($break)
    {
        $this->breakOnChildFail = (bool) $break;
    } // setBreakOnChildFail

    /**
     * Set date/time of last processing (current time).
     *
     * @since 0.1
     */
    public function updateLastProcessed()
    {
        $this->lastProcessed = date('Y-m-d H:i:s');
        if (! $this->firstProcessed) {
            $this->firstProcessed = $this->lastProcessed;
        }
    } // updateLastProcessed

    public function addChild($task)
    {
        if (! $this->ID) {
            throw new \UnexpectedValueException('ID missing (parent task has not been saved yet).');
        }

        $task->setParent($this);
    } // addChild

    public function updateMetaData($key, $value, $global = false)
    {
        if ($global && $this->parent) {
            $this->parent->updateMetaData($key, $value, $global);
        } else {
            $this->metaData[$key] = $value;
        }
    } // updateMetaData

    public function incrementTimesExecuted()
    {
        $this->timesExecuted++;
    } // incrementTimesExecuted

    public function childStatusChanged($child)
    {
        if ($child->getStatus() === static::STATUS_FAILED) {
            $this->childFailed = true;
            if ($this->getBreakOnChildFail()) {
                $this->setStatus(static::STATUS_FAILED);
            }
        }
    } // childStatusChanged

    /**
     * Compose and return data to be stored persistently.
     *
     * @since 0.1
     *
     * @return array Flat associative array of data to store.
     */
    public function getPersistentData()
    {
        $data = array(
            'parent_id' => $this->parent ? $this->parent->getID() : null,
            'queue_group' => $this->parent ? null : $this->queueGroup,
            'status' => $this->status,
            'processor_id' => $this->processorID,
            'created' => $this->created,
            'first_processed' => $this->firstProcessed,
            'last_processed' => $this->lastProcessed,
            'times_executed' => $this->timesExecuted,
            'max_executions' => $this->maxExecutions,
            'timeout' => $this->timeout ? $this->timeout : null,
            'executed' => $this->executed ? 1 : 0,
            'child_failed' => $this->childFailed ? 1 : 0,
            'break_on_child_fail' => $this->breakOnChildFail ? 1 : 0,
            'task_worker_class' => $this->taskWorkerClass,
            'meta_data' => json_encode($this->metaData)
        );

        return $data;
    } // getPersistentData

    /**
     * Populate object with data retrieved by its data mapper.
     *
     * @since 0.1
     *
     * @param array $data Raw task data from DB.
     */
    public function populate($data)
    {
        if (! is_array($data) || count($data) == 0) {
            return;
        }

        foreach ($data as $colName => $value) {
            switch ($colName) {
                case 'id':
                    $this->setID($value);
                    break;
                case 'queue_group':
                    $this->setQueueGroup($value);
                    break;
                case 'status':
                    $this->setStatus($value);
                    break;
                case 'processor_id':
                    $this->setProcessorID($value);
                    break;
                case 'created':
                    $this->created = $value;
                    break;
                case 'first_processed':
                    $this->firstProcessed = $value;
                    break;
                case 'last_processed':
                    $this->lastProcessed = $value;
                    break;
                case 'times_executed':
                    $this->timesExecuted = $value;
                    break;
                case 'max_executions':
                    $this->setMaxExecutions($value);
                    break;
                case 'timeout':
                    $this->setTimeout($value);
                    break;
                case 'executed':
                    $this->setExecuted($value);
                    break;
                case 'child_failed':
                    $this->childFailed = $value ? true : false;
                    break;
                case 'break_on_child_fail':
                    $this->setBreakOnChildFail($value);
                    break;
                case 'task_worker_class':
                    $this->taskWorkerClass = $value;
                    break;
                case 'meta_data':
                    $this->metaData = is_array($value) ? $value : $this->decodeJson($value);
                    break;
            }
        }
    } // populate

    /**
     * Recursively delete the task's and all child task's data.
     *
     * @since 0.1
     */
    public function delete()
    {
        $taskMapper = new TaskMapper();

        while ($childTask = $taskMapper->getNextChild($this)) {
            $childTask->delete();
        }

        $taskMapper->delete($this);
        $this->setID(null);
    } // delete

    /**
     * Convert a (possibly large) JSON string into an array.
     *
     * @since 0.1
     *
     * @param $jsonString JSON-String.
     *
     * @return Associative array.
     */
    private function decodeJson($jsonString)
    {
        $stream = fopen('php://temp', 'r+');
        fwrite($stream, $jsonString);
        rewind($stream);

        $listener = new \JsonStreamingParser\Listener\InMemoryListener();

        try {
            $parser = new \JsonStreamingParser\Parser($stream, $listener);
            $parser->parse();
            fclose($stream);
        } catch (Exception $e) {
            fclose($stream);
            throw $e;
        }

        return $listener->getJson();
    } // decodeJson
} // class Task
