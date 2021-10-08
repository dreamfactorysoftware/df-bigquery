<?php

namespace DreamFactory\Core\BigQuery\Models;

use DreamFactory\Core\Database\Components\SupportsExtraDbConfigs;
use DreamFactory\Core\Models\BaseServiceConfigModel;

class BigQueryConfig extends BaseServiceConfigModel
{
    use SupportsExtraDbConfigs;

    protected $table = 'bigquery_config';

    protected $fillable = ['service_id', 'application_credentials_json', 'project_id', 'auth_cache_store', 'options', 'location'];

    protected $casts = [
        'service_id' => 'integer',
        'options'    => 'array'
    ];

    protected $encrypted = ['application_credentials_json'];

    protected $protected = ['application_credentials_json'];

    /**
     * {@inheritdoc}
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'application_credentials_json':
                $schema['label'] = 'Application Credentials JSON';
                $schema['type'] = 'text';
                $schema['default'] = '';
                $schema['description'] =
                    'Content of the service account key JSON file. You can use service account key files to authenticate an application as a service account.';
                break;
            case 'project_id':
                $schema['label'] = 'Project ID';
                $schema['description'] = 'Your Google Cloud project ID';
                break;
            case 'auth_cache_store':
                $schema['label'] = 'Auth Cache Store';
                $schema['description'] = ' This option controls the auth cache connection that gets used.' .
                'Supported: "apc", "array", "database", "file", "memcached", "redis"';
                break;
            case 'location':
                $schema['label'] = 'Location';
                $schema['description'] = 'Specify the dataset location.' .
                'Supported values can be found at https://cloud.google.com/bigquery/docs/locations';
                break;
            case 'options':
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] =
                    'Client Options. Additional parameters that ' .
                    'the underlying BigQueryClient will use.' .
                    ' Available options are - <br>' .
                    ' - authCacheOptions <br>' .
                    ' - authHttpHandler <br>' .
                    ' - httpHandler <br>' .
                    ' - retries <br>' .
                    ' - scopes <br>' .
                    ' - returnInt64AsObject';
                break;
        }
    }
}