<?php

namespace DreamFactory\Core\BigQuery\Database\Schema;

use DreamFactory\Core\BigQuery\Database\BigQueryConnection;
use DreamFactory\Core\Database\Components\DataReader;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotImplementedException;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use DreamFactory\Core\Database\Components\Schema;

class BigQuerySchema extends Schema
{
    /**
     * @const string Quoting characters
     */
//    const LEFT_QUOTE_CHARACTER = '`';

    /**
     * @const string Quoting characters
     */
//    const RIGHT_QUOTE_CHARACTER = '`';


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

        return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032") . "'";
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $cTable = $this->connection->getClient()->getTable($table->name);
        $columns = $cTable->columns();
        $primaryKeys = $cTable->primaryKey();
        $pkNames = [];
        foreach ($primaryKeys as $pk) {
            $pkNames[] = $pk->name();
        }

        if (!empty($columns)) {
            foreach ($columns as $name => $column) {
                $c = new ColumnSchema([
                    'name' => $name,
                    'is_primary_key' => (in_array($name, $pkNames)) ? true : false,
                    'allow_null' => true,
                    'db_type' => (string)$column->type(),
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
        foreach ($tables as $table) {
            $name = $table->id();
            $resourceName = $name;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
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
        if (null !== array_get($definition, 'new_name') &&
            array_get($definition, 'name') !== array_get($definition, 'new_name')
        ) {
            $cql = 'ALTER TABLE ' .
                $table .
                ' RENAME ' .
                $this->quoteColumnName($column) .
                ' TO ' .
                $this->quoteColumnName(array_get($definition, 'new_name'));
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

        if ($obj = $this->convertToNativeType($value, $field_info->dbType)) {
            return $obj;
        }

        return parent::typecastToNative($value, $field_info, $allow_null);
    }

    protected function unwrapNativeType($value)
    {
        // handle object types returned by driver
        if (is_object($value)) {
            switch ($cassClass = get_class($value)) {
                case 'Cassandra\Uuid': // constructs with same generated string
                    $value = $value->uuid();
                    break;
                case 'Cassandra\Timeuuid': // construct( int $seconds )
//                    $x = $value->time(); // seconds
//                    $y = $value->toDateTime()->format('Y-m-d H:i:s.uO');
                    $value = $value->uuid();
                    break;
                case 'Cassandra\Timestamp': // __construct ( int $seconds, int $microseconds )
//                    $x = $value->time(); // seconds
//                    $y = $value->microtime(false); // microseconds string '0.u seconds'
//                    $z = $value->microtime(true); // string 'seconds.mil' milliseconds
                    $milliseconds = (string)$value; // milliseconds string
                    $add = '.' . substr($milliseconds, -3);

                    // Their toDateTime drops millisecond accuracy, will add it back
                    if (version_compare(PHP_VERSION, '7.0.0', '>=')) {
                        $value = $value->toDateTime()->format('Y-m-d H:i:s.vO'); // milliseconds best accuracy
                        $value = str_replace('.000', $add, $value);
                    } else {
                        $value = $value->toDateTime()->format('Y-m-d H:i:s.uO');
                        $value = str_replace('.000000', $add, $value);
                    }
                    break;
                case 'Cassandra\Date': // construct ( int $seconds)
//                    $x = (string)$value; // crazy class name included
//                    $y = $value->seconds();
                    $value = $value->toDateTime()->format('Y-m-d');
                    break;
                case 'Cassandra\Time': // construct ( int $nanoseconds)
                    // create DateTime using seconds and add the remainder nanoseconds
                    $datetime = new \DateTime('@' . $value->seconds());
                    $nanoseconds = (int)(string)$value; // nanoseconds
                    $remainder = $nanoseconds % 1000000000;
                    $value = $datetime->format('H:i:s') . '.' . str_pad($remainder, 9, '0', STR_PAD_LEFT);
                    break;
                case 'Cassandra\Blob':
//                    $x = (string)$value; // hexadecimal string
//                    $y = $value->bytes(); // hexadecimal string
                    $value = $value->toBinaryString();
                    break;
                case 'Cassandra\Inet':
//                    $x = $value->address();
                    $value = (string)$value;
                    break;
                case 'Cassandra\Decimal':
//                    $x = $value->value(); // string value without scale
//                    $scale = $value->scale();
                    $value = (string)$value; // E notation?
                    break;
                case 'Cassandra\Float':
//                    $x = $value->value(); // shortens based on float() behavior
                    $value = (string)$value;
                    break;
                case 'Cassandra\Bigint':
                case 'Cassandra\Varint':
                    $value = $value->value(); // should be string as these are typically too large for PHP
                    break;
                case 'Cassandra\Smallint':
                case 'Cassandra\Tinyint':
                    $value = $value->value();
                    break;
                case 'Cassandra\Collection': // aka List type
                    $out = [];
                    foreach ($value->values() as $val) {
                        $out[] = $this->typecastToClient($val, $value->type()->valueType()->name());
                    }
                    $value = $out;
                    break;
                case 'Cassandra\Set':
                    $out = [];
                    foreach ($value->values() as $val) {
                        $out[] = $this->typecastToClient($val, $value->type()->valueType()->name());
                    }
                    $value = $out;
                    break;
                case 'Cassandra\Tuple':
                    $out = [];
                    $types = $value->type()->types();
                    foreach ($value->values() as $ndx => $val) {
                        $out[] = $this->typecastToClient($val, $types[$ndx]->name());
                    }
                    $value = $out;
                    break;
                case 'Cassandra\Map':
                    $out = [];
                    $keys = $value->keys();
                    foreach ($value->values() as $ndx => $val) {
                        $out[$this->typecastToClient($keys[$ndx],
                            $value->type()->keyType()->name())] = $this->typecastToClient($val,
                            $value->type()->valueType()->name());
                    }
                    $value = $out;
                    break;
            }
        }

        return $value;
    }

    protected function convertToNativeType($value, $type)
    {
        $simpleType = $type;
        $extra = null;
        if (false !== $pos = strpos($type, '<')) {
            $simpleType = substr($type, 0, $pos);
            $extra = substr($type, $pos + 1, -1); // strip outer <>
        }
        switch (strtolower($simpleType)) {
            // datetime and such
            case DbSimpleTypes::TYPE_DATE:
                if (is_numeric($value)) {
                    return new \Cassandra\Date((int)$value); // must be seconds, check doc as this is weird
                } else {
                    return \Cassandra\Date::fromDateTime(new \DateTime($value));
                }
                break;
            case DbSimpleTypes::TYPE_TIME:
                if (is_numeric($value)) {
                    return new \Cassandra\Time($value); // must be nanoseconds
                } else {
                    if (false !== $pos = strpos($value, '.')) {
                        // string may include nanoseconds
                        $seconds = substr($value, 0, $pos);
                        $nano = substr($value, $pos + 1);
                        $seconds = strtotime('1970-01-01 ' . $seconds);
                        $nanoseconds = $seconds . str_pad($nano, 9, '0', STR_PAD_RIGHT);

                        return new \Cassandra\Time($nanoseconds);
                    } else {
                        return \Cassandra\Time::fromDateTime(new \DateTime($value));
                    }
                }
                break;
            case DbSimpleTypes::TYPE_TIMESTAMP:
                if (is_numeric($value)) {
                    return new \Cassandra\Timestamp((int)$value); // must be seconds
                } elseif (empty($value) || (0 === strcasecmp($value, 'now()'))) {
                    return new \Cassandra\Timestamp();
                } elseif (false !== $seconds = strtotime($value)) {
                    // may have lost millisecond precision here, see if we can make up for it
                    $microseconds = 0;
                    if (false !== $pos = strpos($value, '.')) {
                        $len = (false !== $plus = strpos($value, '+')) ? $plus - ($pos + 1) : null;
                        $micro = '0.' . substr($value, $pos + 1, $len);
                        $microseconds = floatval($micro) * 1000000;
                    }

                    return new \Cassandra\Timestamp($seconds, $microseconds);
                }
                break;
            case DbSimpleTypes::TYPE_TIME_UUID:
                if (is_numeric($value)) {
                    return new \Cassandra\Timeuuid((int)$value); // must be seconds
                } elseif (empty($value) || (0 === strcasecmp($value, 'now()'))) {
                    return new \Cassandra\Timeuuid();
                } elseif (false !== $seconds = strtotime($value)) {
                    return new \Cassandra\Timeuuid($seconds);
                } else {
                    throw new BadRequestException('TIME UUID type can only be set with null, or seconds, or valid formatted time.');
                }
                break;
            case DbSimpleTypes::TYPE_UUID:
                if (empty($value) || (0 === strcasecmp($value, 'uuid()'))) {
                    return new \Cassandra\Uuid(Uuid::uuid4());
                } else {
                    return new \Cassandra\Uuid($value);
                }
            case DbSimpleTypes::TYPE_BINARY:
                return new \Cassandra\Blob((string)$value);

            // some fancy numbers
            case 'var_int':
                return new \Cassandra\Varint((string)$value);
            case DbSimpleTypes::TYPE_BIG_INT:
                return new \Cassandra\Bigint((string)$value);
            case DbSimpleTypes::TYPE_DECIMAL:
                return new \Cassandra\Decimal((string)$value);
            case DbSimpleTypes::TYPE_FLOAT:
                return new \Cassandra\Float($value);
            case DbSimpleTypes::TYPE_SMALL_INT:
                return new \Cassandra\Smallint($value);
            case DbSimpleTypes::TYPE_TINY_INT:
                return new \Cassandra\Tinyint($value);
            case 'list':
                $obj = new \Cassandra\Collection($extra);
                foreach ($value as $val) {
                    if (is_object($val)) {
                        $newVal = $this->convertToNativeType($val, $extra);
                        if (!is_null($newVal)) { // collections don't allow null values
                            $val = $newVal;
                        }
                    }
                    $obj->add($val);
                }

                return $obj;
            case 'set':
                $obj = new \Cassandra\Set($extra);
                foreach ($value as $val) {
                    if (is_object($val)) {
                        $newVal = $this->convertToNativeType($val, $extra);
                        if (!is_null($newVal)) { // collections don't allow null values
                            $val = $newVal;
                        }
                    }
                    $obj->add($val);
                }

                return $obj;
            case 'tuple':
                throw new BadRequestException('Tuple data type not currently supported for write.');
                break;
//                        $types = strtolower(strstr($field_info->dbType, '<'));
//                        $obj = new \Cassandra\Tuple($types);
//
//                        return $obj;
//                $tupleType = \Cassandra\Type::tuple(\Cassandra\Type::text(), \Cassandra\Type::text(),
//                    \Cassandra\Type::int());
//                $tupleType->create('Phoenix', '9042 Cassandra Lane', 85023)
            case 'map':
                $types = array_map('trim', explode(',', strtolower($extra)));
                $obj = new \Cassandra\Map($types[0], $types[1]);
                foreach ($value as $key => $val) {
                    if (is_object($key)) {
                        $newKey = $this->convertToNativeType($key, $types[0]);
                        if (!is_null($newKey)) { // collections don't allow null values
                            $key = $newKey;
                        }
                    }
                    if (is_object($val)) {
                        $newVal = $this->convertToNativeType($val, $types[1]);
                        if (!is_null($newVal)) { // collections don't allow null values
                            $val = $newVal;
                        }
                    }
                    $obj->set($key, $val);
                }

                return $obj;

            // catch any other weird ones
            case 'inet':
                return new \Cassandra\Inet((string)$value);
        }

        return null;
    }
}