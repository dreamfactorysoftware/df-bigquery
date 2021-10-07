<?php

namespace DreamFactory\Core\BigQuery\Services;

use DreamFactory\Core\BigQuery\Resources\Table;
use DreamFactory\Core\BigQuery\Database\Schema\Schema;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Services\BaseDbService;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseRestResource;
use DreamFactory\Core\SqlDb\Services\SqlDb;

/**
 * Class BigQuery
 *
 * @package DreamFactory\Core\BigQuery\Services
 */
class BigQuery extends BaseDbService
{
    use RequireExtensions;

    public function __construct(array $settings)
    {
        parent::__construct($settings);

//        $this->config['driver'] = 'bigquery ';

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
}