<?php

namespace DreamFactory\Core\BigQuery\Components;

use Google\Cloud\BigQuery\BigQueryClient as GoogleClient;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Cache;
use Symfony\Component\Cache\Adapter\Psr16Adapter;

class BigQueryClient
{
    public static function createForConfig(array $bigQueryConfig): GoogleClient
    {
        $options = empty(Arr::get($bigQueryConfig, 'options', [])) ? [] : Arr::get($bigQueryConfig, 'options', []);
        $clientConfig = array_merge([
            'projectId' => $bigQueryConfig['project_id'],
            'keyFile' => json_decode(Arr::get($bigQueryConfig, 'application_credentials_json', null), true),
            'authCache' => self::configureCache($bigQueryConfig['auth_cache_store']),
            'location' => Arr::get($bigQueryConfig, 'location', null),
        ], $options);

        return new GoogleClient($clientConfig);
    }

    protected static function configureCache($cacheStore)
    {
        $store = Cache::store($cacheStore);

        $cache = new Psr16Adapter($store);

        return $cache;
    }
}