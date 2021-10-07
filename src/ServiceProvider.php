<?php
namespace DreamFactory\Core\BigQuery;

use DreamFactory\Core\BigQuery\Database\BigQueryConnection;
use DreamFactory\Core\BigQuery\Models\BigQueryConfig;
use DreamFactory\Core\BigQuery\Services\BigQuery;
use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('bigquery', function ($config) {
                return new BigQueryConnection($config);
            });
        });

        $this->app->resolving('db.schema', function (DbSchemaExtensions $db){
            $db->extend('bigquery', function ($connection){
                return new BigQuerySchema($connection);
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'bigquery',
                    'label'           => 'BigQuery',
                    'description'     => 'Service for Google Cloud BigQuery connections.',
                    'group'           => 'Big Data',
                    'config_handler'  => BigQueryConfig::class,
                    'factory'         => function ($config) {
                        return new BigQuery($config);
                    },
                ])
            );
        });
    }

    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}
