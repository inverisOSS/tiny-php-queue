<?php
namespace inverisOSS\TinyPHPQueue;

use inverisOSS\TinyPHPQueue\Task;
use inverisOSS\TinyPHPQueue\Exceptions\ConfigException;

use inverisOSS\TinyPHPQueue\Exceptions\DBException;

class TaskMapper
{
    private $db;

    public function __construct()
    {
        $this->db = Config::get('db');
        if (!$this->db) {
            throw new ConfigException('PDO DB connection object missing in configuration.');
        }
    } // __construct

    /**
     * Create and return a task object based on its DB data.
     *
     * @since 0.1
     *
     * @param int $id Task ID
     *
     * @return Task|int Task object or false if nonexistent.
     */
    public function getByID($id)
    {
        $taskRecord = $this->load($id);
        if (!$taskRecord) {
            return false;
        }

        $task = new Task();
        $task->populate($taskRecord);

        if ($taskRecord['parent_id']) {
            $parent = $this->getByID($taskRecord['parent_id']);
            if ($parent) {
                $task->setParent($parent);
            }
        }

        return $task;
    } // getByID

    /**
     * Create and return an array of task objects based on one or multiple query arguments.
     *
     * @since 0.1
     *
     * @param array $args Associative array of query parameters
     *                    ('column_name' => array('value' => 'xy', 'compare' => '=')).
     * @param array $pagination Associative array of pagination parameters (page, tasks_per_page).
     *
     * @return Task[] Array including two child arrays: tasks = task objects,
     *                pagination = current pagination data.
     */
    public function getAllBy($args = null, $pagination = null)
    {
        if (!$pagination || (is_array($pagination) && count($pagination) === 0)) {
            $pagination = array(
                'tasks_per_page' => 0, // CHECK: Config::DEFAULT_DB_TASK_QUERY_LIMIT
                'page' => 1
            );
        } else {
            if (!isset($pagination['page']) || (int) $pagination['page'] <= 0) {
                $pagination['page'] = 1;
            }
            if (!isset($pagination['tasks_per_page'])) {
                $pagination['tasks_per_page'] = Config::DEFAULT_DB_TASK_QUERY_LIMIT;
            }
        }
        $tasks = array();

        /**
         * Generate SQL query string and parameters.
         */
        $select = array('id');
        $where = array();
        $having = array();
        $queryData = array();

        if (is_array($args) && count($args) > 0) {
            foreach ($args as $fieldName => $fieldData) {
                if (is_string($fieldData)) {
                    $fieldData = array( 'value' => $fieldData );
                }

                if ($fieldName === 'meta_data') {
                    foreach ($args['meta_data'] as $key => $metaQueryData) {
                        if (is_array($metaQueryData)) {
                            if (!isset($metaQueryData['value'])) {
                                continue;
                            }
                            $value = $metaQueryData['value'];
                            $compare = isset($metaQueryData['compare']) ? $metaQueryData['compare'] : '=';
                        } else {
                            $value = $metaQueryData;
                            $compare = '=';
                        }

                        $select[] = <<<EOT
REPLACE(
    SUBSTR(
        SUBSTR(
            meta_data,
            INSTR(
                meta_data,
                '"$key":'
            )
        ),
        LENGTH('"$key":') + 1,
        INSTR(
            REPLACE(
                SUBSTR(
                    meta_data,
                    INSTR(meta_data, '"$key":')
                ),
                '}',
                ',"'
            ),
            ','
        ) - LENGTH('"$key":') - 1
    ),
    '"',
    ''
) AS `meta_$key`
EOT;

                        $having[] = "meta_$key $compare :meta_$key";
                        $queryData["meta_$key"] = trim(json_encode($value), '"');
                    }
                } else {
                    $compare = isset($fieldData['compare']) && $fieldData['compare'] ? $fieldData['compare'] : '=';

                    if (strtoupper($compare) === 'IN') {
                        if (is_string($fieldData['value'])) {
                            $fieldData['value'] = array_map('trim', explode(',', $fieldData['value']));
                        }
                        $inValues = "'" . implode("','", $fieldData['value']) . "'";

                        $where[] = $fieldName . " $compare (" . $inValues . ')';
                    } else {
                        $where[] = $fieldName . " $compare :" . $fieldName;
                        $queryData[$fieldName] = $fieldData['value'];
                    }
                }
            }
        }

        $query = sprintf('SELECT ' . implode(', ', $select) . ' FROM %s', Config::QDB_TASK_TABLE);
        if (count($where) > 0) {
            $query .= ' WHERE ' . implode(' AND ', $where);
        }
        if (count($having) > 0) {
            $query .= ' GROUP BY id HAVING ' . implode(' AND ', $having);
        }

        $unlimitedQuery = str_replace('SELECT id', 'SELECT count(id)', $query);

        if ($pagination['tasks_per_page'] !== 0) {
            $offset = ($pagination['page'] - 1) * $pagination['tasks_per_page'];
            $query .= " LIMIT $offset, " . $pagination['tasks_per_page'];
        }

        try {
            $stmt = $this->db->prepare($query);
            $stmt->execute($queryData);

            $stmtCount = $this->db->prepare($unlimitedQuery);
            $stmtCount->execute($queryData);
            $totalTasksCount = $stmtCount->fetch(\PDO::FETCH_COLUMN);

            $pages = $pagination['tasks_per_page'] > 0 ? ceil($totalTasksCount / $pagination['tasks_per_page']) : 1;
            if ($pagination['page'] > $pages) {
                $pagination['tasks_per_page'] = $pages;
            }
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $taskRecord = $this->load($row['id']);
            $task = new Task();
            $task->populate($taskRecord);
            if ($taskRecord['parent_id']) {
                $parent = $this->getByID($taskRecord['parent_id']);
                if ($parent) {
                    $task->setParent($parent);
                }
            }
            $tasks[$row['id']] = $task;
        }

        return array(
            'tasks' => $tasks,
            'pagination' => array(
                'tasks_per_page' => $pagination['tasks_per_page'],
                'page' => $pagination['page'],
                'pages' => $pages
            )
        );
    } // getAllBy

    /**
     * Get the next processable task.
     *
     * @since 0.1
     *
     * @param int $id Task ID.
     * @param string $queueGroup Queue group name.
     *
     * @return Task|bool Next processable task or false if nonexistent.
     */
    public function getNextProcessableTask($processorID, $queueGroup = Config::DEFAULT_QUEUE_GROUP)
    {
        $query = sprintf(
            'SELECT id FROM %1$s
                WHERE (status="%2$s" OR (status="%3$s" AND processor_id="%4$s"))
                AND queue_group="%5$s" ORDER BY CASE WHEN status="%3$s" THEN 1 ELSE 2 END, id ASC
                LIMIT 1',
            Config::QDB_TASK_TABLE,
            Task::STATUS_PENDING,
            Task::STATUS_PROCESSING,
            $processorID,
            $queueGroup
        );

        try {
            $stmt = $this->db->prepare($query);
            $stmt->execute();
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        $task = $result ? $this->getByID($result['id']) : false;

        return $task;
    } // getNextProcessableTask

    /**
     * Get the next processable child task.
     *
     * @since 0.1
     *
     * @param Task $task Parent task object.
     * @param bool $processable Processable child tasks
     *     only (pending/processing, default: true)?
     *
     * @return Task|bool Next child task of false if nonexistent.
     */
    public function getNextChild(Task $task, $processable = true)
    {
        if (!$task->getID()) {
            return false;
        }

        $queryData = array(
            'parent_id' => $task->getID()
        );

        if ($processable) {
            $query = sprintf(
                'SELECT id FROM %s
                    WHERE parent_id=:parent_id AND (status="%2$s" OR status="%3$s")
                    ORDER BY CASE WHEN status="%3$s" THEN 1 ELSE 2 END, id ASC
                    LIMIT 1',
                Config::QDB_TASK_TABLE,
                Task::STATUS_PENDING,
                Task::STATUS_PROCESSING
            );
        } else {
            $query = sprintf(
                'SELECT id FROM %s WHERE parent_id=:parent_id ORDER BY id ASC LIMIT 1',
                Config::QDB_TASK_TABLE
            );
        }

        try {
            $stmt = $this->db->prepare($query);
            $stmt->execute($queryData);
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        $child = $result ? $this->getByID($result['id']) : false;

        return $child;
    } // getNextChild

    /**
     * Save a task's data.
     *
     * @since 0.1
     *
     * @param Task $task Task object to save.
     *
     * @return int ID of the saved task.
     */
    public function save(Task $task)
    {
        $taskID = $task->getID();
        $data = $task->getPersistentData();

        if ($taskID) {
            $data['id'] = $taskID;
        }

        if ($taskID === null) {
            // Insert a new task.
            $fieldList = implode(', ', array_keys($data));
            $fieldNameList = ':' . implode(', :', array_keys($data));
        } else {
            // Update an existing task.
            $updateFields = '';
            foreach (array_keys($data) as $fieldName) {
                if ($fieldName === 'id') {
                    continue;
                }
                $updateFields .= "$fieldName=:$fieldName, ";
            }
            $updateFields = substr($updateFields, 0, -2);
        }

        try {
            if ($taskID === null) {
                $query = sprintf('INSERT INTO %s (%s) VALUES (%s)', Config::QDB_TASK_TABLE, $fieldList, $fieldNameList);
            } else {
                $query = sprintf('UPDATE %s SET %s WHERE id=:id', Config::QDB_TASK_TABLE, $updateFields);
            }

            $stmt = $this->db->prepare($query);
            $stmt->execute($data);
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        if (!$taskID) {
            $task->setID($this->db->lastInsertId());
        }

        if ($task->getParent()) {
            $this->save($task->getParent());
        }

        return $task->getID();
    } // save

    /**
     * Delete a task's data.
     *
     * @since 0.1
     *
     * @param Task $task Task object to delete.
     *
     * @return bool True on success, false on error or if ID not set.
     */
    public function delete(Task $task)
    {
        $taskID = $task->getID();
        if (!$taskID) {
            return false;
        }

        $query = sprintf('DELETE FROM %s WHERE id=:id', Config::QDB_TASK_TABLE);

        try {
            $stmt = $this->db->prepare($query);
            $stmt->execute(array('id' => $taskID));
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        return true;
    } // delete

    /**
     * Delete expired tasks.
     *
     * @since 1.0
     *
     * @param string $expiredBefore Date/Time statement for strtotime.
     *
     * @return mixed[] Array with actual expiry date/time string and number of
     *     deleted main tasks.
     */
    public function deleteExpiredTasks($expiredBefore)
    {
        $expiryTS = strtotime($expiredBefore);
        // Possibly convert future to past expiry date/time.
        if ($expiryTS > time()) {
            $expiryTS = $strtotime('-' . $expiredBefore);
        }

        $query = sprintf(
            'SELECT id FROM %s
                WHERE (parent_id IS NULL OR parent_id = 0)
                AND (status = "%2$s" OR status = "%3$s")
                AND (created < "%4$s")
                ORDER BY created ASC',
            Config::QDB_TASK_TABLE,
            Task::STATUS_DONE,
            Task::STATUS_FAILED,
            date('Y-m-d H:i:s', $expiryTS)
        );
        $deletedCount = 0;

        try {
            $stmt = $this->db->prepare($query);
            $stmt->execute();

            while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $taskRecord = $this->load($row['id']);
                $task = new Task();
                $task->populate($taskRecord);
                $task->delete();
                if ($task->getID() === null) {
                    $deletedCount++;
                }
            }
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        return [
            'expiry_date_time' => date('Y-m-d H:i:s', $expiryTS),
            'deleted_count' => $deletedCount
        ];
    } // delete_expired_tasks

    /**
     * Fetch a task data record from DB.
     *
     * @since 0.1
     * @access private
     *
     * @param int $id Task ID.
     *
     * @return array|bool Task data record as associative array or false if nonexistent.
     */
    private function load($id)
    {
        $queryData = array(
            'id' => $id
        );

        try {
            $query = sprintf('SELECT * FROM %s WHERE id=:id', Config::QDB_TASK_TABLE);
            $stmt = $this->db->prepare($query);
            $stmt->execute($queryData);
            $taskRecord = $stmt->fetch(\PDO::FETCH_ASSOC);
        } catch (\PDOException $e) {
            throw new DBException(sprintf('PDO Error: %s', $e->getMessage()), null, $e); // @codeCoverageIgnore
        }

        return $taskRecord;
    } // load
} // class TaskMapper
