<?php
namespace inverisOSS\TinyPHPQueue\tests;

class ArrayDataSet extends \PHPUnit\DbUnit\DataSet\AbstractDataSet
{
    protected $tables = array();

    /**
     * @param array $data
     */
    public function __construct(array $data)
    {
        foreach ($data as $tableName => $rows) {
            $columns = array();
            if (isset($rows[0])) {
                $columns = array_keys($rows[0]);
            }

            $metaData = new \PHPUnit\DbUnit\DataSet\DefaultTableMetaData($tableName, $columns);
            $table = new \PHPUnit\DbUnit\DataSet\DefaultTable($metaData);

            foreach ($rows as $row) {
                $table->addRow($row);
            }
            $this->tables[$tableName] = $table;
        }
    } // __construct

    protected function createIterator($reverse = false)
    {
        return new \PHPUnit\DbUnit\DataSet\DefaultTableIterator($this->tables, $reverse);
    } // __createIterator

    public function getTable($tableName)
    {
        if (!isset($this->tables[$tableName])) {
            throw new \InvalidArgumentException("$tableName is not a table in the current database.");
        }

        return $this->tables[$tableName];
    } // getTable
} // class ArrayDataSet
