<?php

namespace DreamFactory\Core\BigQuery\Database;

use DreamFactory\Core\BigQuery\Components\BigQueryClient;
use DreamFactory\Core\BigQuery\Database\Query\BigQueryBuilder;
use DreamFactory\Core\BigQuery\Database\Query\Grammars\BigQueryGrammar;
use DreamFactory\Core\BigQuery\Database\Query\Processors\BigQueryProcessor;
use Illuminate\Database\Connection;

class BigQueryConnection extends Connection
{
    /** @type BigQueryClient */
    protected $client;

    public function __construct(array $config)
    {
        $this->client = BigQueryClient::createForConfig($config);
        $this->useDefaultPostProcessor();
        $this->useDefaultQueryGrammar();
    }

    /**
     * @return \DreamFactory\Core\BigQuery\Database\Query\Processors\BigQueryProcessor
     */
    public function getDefaultPostProcessor()
    {
        return new BigQueryProcessor();
    }

    /**
     * @return \DreamFactory\Core\BigQuery\Database\Query\Grammars\BigQueryGrammar
     */
    public function getDefaultQueryGrammar()
    {
        return new BigQueryGrammar();
    }

    /**
     * @return \DreamFactory\Core\BigQuery\Components\BigQueryClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param \Closure|\Illuminate\Database\Query\Builder|string $table
     * @param string|null $as
     * @return \Illuminate\Database\Query\Builder
     */
    public function table($table, $as = null)
    {
        $processor = $this->getPostProcessor();
        $grammar = $this->getQueryGrammar();

        $query = new BigQueryBuilder($this, $grammar, $processor);
        $result = $query->from($table, $as);
        return $result;
    }

    /**
     * @param string $query
     * @param array $bindings
     * @param bool $useReadPdo
     *
     * @return mixed
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        return $this->statement($query, $bindings);
    }

    /**
     * Run an insert statement against the database.
     *
     * @param string $query
     * @param array $bindings
     *
     * @return bool
     * @throws InternalServerErrorException
     */
    public function insert($query, $bindings = [])
    {
        try {
            $this->statement($query, $bindings);

            return true;
        } catch (\Exception $e) {
            throw new InternalServerErrorException('Insert failed. ' . $e->getMessage());
        }
    }

    /**
     * Run an update statement against the database.
     *
     * @param string $query
     * @param array $bindings
     *
     * @return bool
     * @throws InternalServerErrorException
     */
    public function update($query, $bindings = [])
    {
        try {
            $this->statement($query, $bindings);

            return true;
        } catch (\Exception $e) {
            throw new InternalServerErrorException('Update failed. ' . $e->getMessage());
        }
    }

    /**
     * Run a delete statement against the database.
     *
     * @param string $query
     * @param array $bindings
     *
     * @return bool
     * @throws InternalServerErrorException
     */
    public function delete($query, $bindings = [])
    {
        try {
            $this->statement($query, $bindings);

            return true;
        } catch (\Exception $e) {
            throw new InternalServerErrorException('Update failed. ' . $e->getMessage());
        }
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param string $query
     * @param array $bindings
     *
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        $queryJobConfig = $this->client->query($query);

        if (!empty($bindings)) {
            return $this->client->runQuery($queryJobConfig, ['arguments' => $bindings]);
        } else {
            return $this->client->runQuery($queryJobConfig);
        }
    }
}