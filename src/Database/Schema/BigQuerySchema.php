<?php

namespace DreamFactory\Core\BigQuery\Database\Schema;

use DreamFactory\Core\BigQuery\Database\BigQueryConnection;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Database\Components\Schema;
use Arr;

class BigQuerySchema extends Schema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '`';

    /**
     * @const string Quoting characters
     */
    const RIGHT_QUOTE_CHARACTER = '`';


    /** @var  BigQueryConnection */
    protected $connection;

    /**
     * Quotes a string value for use in a query.
     *
     * @param string $str string to be quoted
     *
     * @return string the properly quoted string
     * @see http://www.php.net/manual/en/function.PDO-quote.php
     */
    public function quoteValue($str)
    {
        if (is_int($str) || is_float($str)) {
            return $str;
        }

        return "`" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032") . "`";
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $client = $this->connection->getClient();
        $dataset = $client->dataset($table->schemaName);
        $cTable = $dataset->table($table->resourceName);
        $columns = Arr::get($cTable->info(), 'schema.fields');
        if (!empty($columns)) {
            foreach ($columns as $column) {
                $c = new ColumnSchema([
                    'name' => Arr::get($column, 'name'),
                    'is_primary_key' => false, // no primary keys in google's bigquery
                    // REQUIRED => NOT NULL; NULLABLE/REPEATED => nullable
                    'allow_null' => Arr::get($column, 'mode') !== 'REQUIRED',
                    'db_type' => Arr::get($column, 'type'),
                ]);
                $c->quotedName = $this->quoteColumnName($c->name);
                $this->extractType($c, $c->dbType);
                $table->addColumn($c);
            }
        }
    }

    public function getSchemas()
    {
        $client = $this->connection->getClient();
        $datasets = $client->datasets();
        $schemas = [];

        foreach ($datasets as $dataset) {
            $schemaName = $dataset->id();
            $schemas[] = $schemaName;
        }

        return $schemas;
    }

    /**
     * Returns all table names in the database.
     *
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned table names will be prefixed with the schema name.
     *
     * @return array all table names in the database.
     */
    protected function getTableNames($schema = '')
    {
        $client = $this->connection->getClient();
        $dataset = $client->dataset($schema);
        $names = [];
        $tables = $dataset->tables();
        $schemaName = $dataset->id();
        $projectId = Arr::get($dataset->identity(), 'projectId');
        foreach ($tables as $table) {
            $name = $table->id();
            $resourceName = $name;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
            $settings = compact('projectId', 'schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getViewNames(
        /** @noinspection PhpUnusedParameterInspection */
        $schema = ''
    )
    {

    }

    /**
     * @param array $info
     *
     * @return string
     * @throws \Exception
     */
    protected function buildColumnDefinition(array $info)
    {
        // This works for most except Oracle
        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        if ('string' === $definition) {
            $definition = 'text';
        }

        //$allowNull = (isset($info['allow_null'])) ? $info['allow_null'] : null;
        //$definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['db_type'])) ? $info['db_type'] : null;
        if (isset($default)) {
            if (is_array($default)) {
                $expression = (isset($default['expression'])) ? $default['expression'] : null;
                if (null !== $expression) {
                    $definition .= ' DEFAULT ' . $expression;
                }
            } else {
                $default = $this->quoteValue($default);
                $definition .= ' DEFAULT ' . $default;
            }
        }

        if (isset($info['is_primary_key']) && filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' PRIMARY KEY';
        } elseif (isset($info['is_unique']) && filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN)) {
            throw new BadRequestException('Unique constraints are not currently supported for BigQuery.');
        }

        return $definition;
    }

    public function addColumn($table, $column, $type)
    {
        return <<<CQL
ALTER TABLE $table ADD {$this->quoteColumnName($column)} {$this->getColumnType($type)};
CQL;
    }

    /**
     * @inheritdoc
     */
    public function alterColumn($table, $column, $definition)
    {
        if (null !== Arr::get($definition, 'new_name') &&
            Arr::get($definition, 'name') !== Arr::get($definition, 'new_name')
        ) {
            $cql = 'ALTER TABLE ' .
                $table .
                ' RENAME ' .
                $this->quoteColumnName($column) .
                ' TO ' .
                $this->quoteColumnName(Arr::get($definition, 'new_name'));
        } else {
            $cql = 'ALTER TABLE ' .
                $table .
                ' ALTER ' .
                $this->quoteColumnName($column) .
                ' TYPE ' .
                $this->getColumnType($definition);
        }

        return $cql;
    }

    /**
     * @inheritdoc
     */
    public function dropColumns($table, $columns)
    {
        $columns = (array)$columns;

        if (!empty($columns)) {
            return $this->connection->statement("ALTER TABLE $table DROP " . implode(',', $columns));
        }

        return false;
    }

    public function typecastToClient($value, $field_info, $allow_null = true)
    {
        return parent::typecastToClient($this->unwrapNativeType($value), $field_info, $allow_null);
    }

    public function typecastToNative($value, $field_info, $allow_null = true)
    {
        if (is_null($value) && $field_info->allowNull) {
            return null;
        }

        // BigQuery DML runs through parameterized query jobs; the Google client's
        // ValueMapper maps PHP scalars and DateTime values to the correct BigQuery
        // parameter type. So we normalize to a plain scalar via the base typecaster
        // and let the SDK handle the native mapping (no driver value objects here,
        // unlike the SQL/PDO connectors this was originally forked from).
        return parent::typecastToNative($value, $field_info, $allow_null);
    }

    /**
     * Normalize the value objects the BigQuery client returns for temporal, numeric
     * and byte columns (Google\Cloud\BigQuery\{Date,Time,Timestamp,Numeric,BigNumeric,
     * Bytes,Geography} and \DateTime for DATETIME) into plain scalars for JSON output.
     * Plain scalars pass through untouched.
     *
     * @param mixed $value
     * @return mixed
     */
    protected function unwrapNativeType($value)
    {
        if (!is_object($value)) {
            return $value;
        }

        // Every BigQuery value object exposes formatAsString() (Date/Time/Timestamp/
        // Numeric/BigNumeric/Bytes/Geography).
        if (method_exists($value, 'formatAsString')) {
            return $value->formatAsString();
        }

        // DATETIME columns come back as a plain \DateTime.
        if ($value instanceof \DateTimeInterface) {
            return $value->format(\DateTime::ATOM);
        }

        if (method_exists($value, '__toString')) {
            return (string)$value;
        }

        return $value;
    }
}