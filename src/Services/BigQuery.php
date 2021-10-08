<?php

namespace DreamFactory\Core\BigQuery\Services;

use DreamFactory\Core\BigQuery\Database\Schema\BigQuerySchema;
use DreamFactory\Core\BigQuery\Resources\Table;
use DreamFactory\Core\Database\Services\BaseDbService;

/**
 * Class BigQuery
 *
 * @package DreamFactory\Core\BigQuery\Services
 */
class BigQuery extends BaseDbService
{
    public function __construct(array $settings)
    {
        parent::__construct($settings);

        $this->config['driver'] = 'bigquery';

        $prefix = '';
        $parts = ['project_id'];
        foreach ($parts as $part) {
            $prefix .= array_get($this->config, $part);
        }

        $this->setConfigBasedCachePrefix($prefix . ':');
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();
        $handlers[Table::RESOURCE_NAME] = [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ];

        return $handlers;
    }

    protected function initializeConnection()
    {
        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $this->config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->schema = new BigQuerySchema($this->dbConn);
    }
}