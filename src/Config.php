<?php
namespace inverisOSS\TinyPHPQueue;

use inverisOSS\TinyPHPQueue\Task;

abstract class Config
{
    const
        DEFAULT_QUEUE_GROUP = 'default',
        QDB_TASK_TABLE = 'queue',
        DEFAULT_TASK_STATUS = Task::STATUS_PENDING,
        DEFAULT_DB_TASK_QUERY_LIMIT = 20;

    public static $registry;

    /**
     * Return an element of the config registry.
     *
     * @since 0.1
     *
     * @param string $key Config key.
     *
     * @return mixed Config value or false if nonexistent.
     */
    public static function get($key)
    {
        if (isset(self::$registry[$key])) {
            return self::$registry[$key];
        } else {
            return false;
        }
    } // get

    /**
     * Add or update an element of the the config registry.
     *
     * @since 0.1
     *
     * @param string $key Config key.
     * @param string $value Related value.
     */
    public static function set($key, $value)
    {
        self::$registry[$key] = $value;
    } // set
} // class Config
