<?php

namespace Aracool\Logger;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Logger;
use PDO;
use PDOStatement;
use Ramsey\Uuid\Uuid;

/**
 * Class MySQLHandler
 * @package Aracool\Logger
 */
class MysqlHandler extends AbstractProcessingHandler
{
    /**
     * @var bool defines whether the MySQL connection is been initialized
     */
    private $initialized = false;

    /**
     * @var PDO pdo object of database connection
     */
    protected $pdo;

    /**
     * @var PDOStatement statement to insert a new record
     */
    private $statement;

    /**
     * @var string the table to store the logs in
     */
    private $table = 'logs';

    /**
     * @var array default fields that are stored in db
     */
    private $defaultFields = array('id', 'channel', 'level', 'level_name', 'trace', 'payload', 'message', 'time');
    /**
     * @var array
     */
    private $defaultContextFields = ['trace' => null, 'payload' => null];
    /**
     * @var array
     */
    private $fields = array();

    /**
     * MysqlHandler constructor.
     * @param PDO|null $pdo
     * @param          $table
     * @param int      $level
     * @param bool     $bubble
     */
    public function __construct(
        PDO $pdo = null,
        $table,
        $level = Logger::DEBUG,
        $bubble = true
    )
    {
        if (!is_null($pdo)) {
            $this->pdo = $pdo;
        }
        $this->table = $table;
        parent::__construct($level, $bubble);
    }

    /**
     * Initializes this handler by creating the table if it not exists
     */
    private function initialize()
    {
        $this->pdo->exec(
            'CREATE TABLE IF NOT EXISTS ' . $this->table . '
(
    id VARCHAR(30) PRIMARY KEY NOT NULL,
    channel VARCHAR(60) NOT NULL,
    level INT NOT NULL,
    level_name VARCHAR(10) NOT NULL,
    message VARCHAR(250) NOT NULL,
    trace TEXT,
    payload TEXT,
    time DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_channel_index ON ' . $this->table . ' (channel) USING HASH;
CREATE INDEX IF NOT EXISTS logs_level_index ON ' . $this->table . ' (level) USING HASH;
CREATE INDEX IF NOT EXISTS logs_time_index ON ' . $this->table . ' (time) USING HASH;'
        );

        $this->initialized = true;
    }

    /**
     * Prepare the sql statement depending on the fields that should be written to the database
     */
    private function prepareStatement()
    {
        $columns = '';
        $fields  = '';
        foreach ($this->fields as $key => $f) {
            if ($key == 0) {
                $columns .= "$f";
                $fields  .= ":$f";
                continue;
            }

            $columns .= ", $f";
            $fields  .= ", :$f";
        }

        $this->statement = $this->pdo->prepare(
            'INSERT INTO `' . $this->table . '` (' . $columns . ') VALUES (' . $fields . ')'
        );
    }


    /**
     * Writes the record down to the log of the implementing handler
     *
     * @param  $record []
     * @return void
     */
    protected function write(array $record): void
    {
        if (!$this->initialized) {
            $this->initialize();
        }

        /**
         * reset $fields with default values
         */
        $this->fields      = $this->defaultFields;
        $record['context'] = array_merge($this->defaultContextFields, $record['context']);

        //'context' contains the array
        $contentArray = [
            'id'           => base64_encode(Uuid::uuid4()->getBytes()),
            'channel'      => $record['channel'],
            'level'        => $record['level'],
            'level_name'   => $record['level_name'],
            'message'      => $record['message'],
            'trace' => $record['context']['trace'],
            'payload'      => is_null($record['context']['payload']) ? null : json_encode($record['context']['payload']),
            'time'         => $record['datetime']->format('Y-m-d H:i:s')
        ];

        // unset array keys that are passed put not defined to be stored, to prevent sql errors
        foreach ($contentArray as $key => $context) {
            if (!in_array($key, $this->fields)) {
                unset($contentArray[$key]);
                unset($this->fields[array_search($key, $this->fields)]);
                continue;
            }

            if ($context === null) {
                unset($contentArray[$key]);
                unset($this->fields[array_search($key, $this->fields)]);
            }
        }

        $this->prepareStatement();
        $this->statement->execute($contentArray);
    }
}
