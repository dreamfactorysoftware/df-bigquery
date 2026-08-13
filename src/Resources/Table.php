<?php
namespace DreamFactory\Core\BigQuery\Resources;

use DB;
use DreamFactory\Core\Database\Enums\DbFunctionUses;
use DreamFactory\Core\Database\Resources\BaseDbTableResource;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Utility\Session;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder;
use Arr;

class Table extends BaseDbTableResource
{
    /**
     * Filter-based UPDATE. BigQuery has no primary keys and requires a WHERE clause on
     * DML, so update runs by filter (not by id). Reads the affected rows back after the
     * update for the response. Uses separate builders so the positional bindings of the
     * UPDATE and the follow-up SELECT never mix.
     *
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');
        $ssFilters = Arr::get($extras, 'ss_filters');

        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }
            if (empty($filter)) {
                throw new BadRequestException('Filter for update request can not be empty; BigQuery requires a WHERE clause.');
            }

            $fieldsInfo = $tableSchema->getColumns(true);
            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);

            if (!empty($parsed)) {
                $updateBuilder = $this->parent->getConnection()->table($tableSchema->internalName);
                $this->convertFilterToNative($updateBuilder, $filter, $params, $ssFilters, $fieldsInfo);
                $updateBuilder->update($parsed);
            }

            $selectBuilder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($selectBuilder, $filter, $params, $ssFilters, $fieldsInfo);

            return $this->runQuery($table, $selectBuilder, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        return $this->updateRecordsByFilter($table, $record, $filter, $params, $extras);
    }

    /**
     * Filter-based DELETE. Captures the matching rows for the response before deleting.
     *
     * {@inheritdoc}
     */
    public function deleteRecordsByFilter($table, $filter, $params = [], $extras = [])
    {
        if (empty($filter)) {
            throw new BadRequestException('Filter for delete request can not be empty; BigQuery requires a WHERE clause.');
        }
        $ssFilters = Arr::get($extras, 'ss_filters');

        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }
            $fieldsInfo = $tableSchema->getColumns(true);

            // grab the rows before they're gone
            $selectBuilder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($selectBuilder, $filter, $params, $ssFilters, $fieldsInfo);
            $results = $this->runQuery($table, $selectBuilder, $extras);

            $deleteBuilder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($deleteBuilder, $filter, $params, $ssFilters, $fieldsInfo);
            $deleteBuilder->delete();

            return $results;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * Commit the batched id-based operations (PUT/PATCH/DELETE/GET by id or ?ids=).
     * BigQuery has no primary keys, so id-based ops require an explicit id_field; the
     * batched ids become a WHERE ... IN (...) filter and run as DML. Unlike the SQL
     * connectors, we do NOT assert affected-rowcount == id-count: BigQuery enforces no
     * uniqueness, so a filter may legitimately match zero or many rows per id.
     *
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        // POST/insert is handled inline in addToTransaction and leaves nothing batched.
        if (empty($this->batchIds)) {
            return null;
        }

        /** @type ColumnSchema $idName */
        $idName = (isset($this->tableIdsInfo[0])) ? $this->tableIdsInfo[0] : null;
        if (empty($idName)) {
            throw new BadRequestException(
                'BigQuery has no primary keys; supply an id_field to update, delete or fetch by id, or use a filter.'
            );
        }

        $extras = (array)$extras;
        $ssFilters = Arr::get($extras, 'ss_filters');
        $updates = Arr::get($extras, 'updates');
        $ids = $this->batchIds;

        // Fresh builder per statement so the positional bindings of a mutation and its
        // follow-up SELECT never mix.
        $matched = function () use ($idName, $ids, $ssFilters) {
            $b = $this->parent->getConnection()->table($this->transactionTableSchema->internalName);
            $b->whereIn($idName->name, $ids);
            $serverFilter = $this->buildQueryStringFromData($ssFilters);
            if (!empty($serverFilter)) {
                Session::replaceLookups($serverFilter);
                $params = [];
                $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
                $b->whereRaw($filterString, $params);
            }

            return $b;
        };

        $out = [];
        switch ($this->getAction()) {
            case Verbs::PUT:
            case Verbs::PATCH:
                if (!empty($updates)) {
                    $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                    if (!empty($parsed)) {
                        $matched()->update($parsed);
                    }
                }
                $out = $this->runQuery($this->transactionTable, $matched(), $extras);
                break;

            case Verbs::DELETE:
                $out = $this->runQuery($this->transactionTable, $matched(), $extras);
                $matched()->delete();
                break;

            case Verbs::GET:
                $out = $this->runQuery($this->transactionTable, $matched(), $extras);
                break;

            default:
                break;
        }

        if (empty($out)) {
            foreach ($ids as $id) {
                $out[] = [$idName->getName(true) => $id];
            }
        }

        $this->batchIds = [];

        return $out;
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array|\DreamFactory\Core\Database\Schema\ColumnSchema[]
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        // BigQuery has no primary keys, so an id can only come from an explicit
        // id_field on the request. With none requested, there are no id columns
        // (getPrimaryKeys returns []); update/delete-by-id therefore require id_field.
        $idsInfo = [];
        if (empty($requested_fields)) {
            // BigQuery has no primary keys, so with no explicit id_field there are
            // no id columns to derive (the old base's getPrimaryKeys() is gone anyway).
            $requested_fields = [];
        } else {
            if (false !== $requested_fields = static::validateAsArray($requested_fields, ',')) {
                foreach ($requested_fields as $field) {
                    $ndx = strtolower($field);
                    if (isset($fields_info[$ndx])) {
                        $idsInfo[] = $fields_info[$ndx];
                    }
                }
            }
        }

        return $idsInfo;
    }

    /**
     * BigQuery write path. Unlike the SQL/PDO connectors this was forked from,
     * BigQuery has no auto-increment/lastInsertId and no client transactions, so
     * INSERT runs here per-record and the record is echoed back (its return value
     * becomes the per-record result in createRecords). Ids are client-supplied.
     * PUT/PATCH/DELETE by id are queued for commitTransaction (see phase notes).
     *
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        /** @noinspection PhpUnusedParameterInspection */
        $rollback = false,
        /** @noinspection PhpUnusedParameterInspection */
        $continue = false,
        /** @noinspection PhpUnusedParameterInspection */
        $single = false
    ) {
        $ssFilters = Arr::get($extras, 'ss_filters');

        if (Verbs::POST === $this->getAction()) {
            $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
            if (empty($parsed)) {
                throw new BadRequestException('No valid fields were found in record.');
            }

            $builder = $this->parent->getConnection()->table($this->transactionTableSchema->internalName);
            if (!$builder->insert($parsed)) {
                throw new InternalServerErrorException('Record insert failed.');
            }

            return $record;
        }

        // PUT/PATCH/DELETE: queue for commitTransaction.
        if (!is_null($record)) {
            $this->batchRecords[] = $record;
        }
        if (!is_null($id)) {
            $this->batchIds[] = $id;
        }

        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $ssFilters = Arr::get($extras, 'ss_filters');

        try {
            $tableSchema = $this->parent->getTableSchema($table);
            if (!$tableSchema) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }

            $fieldsInfo = $tableSchema->getColumns(true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->projectId . '.' . $tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            return $this->runQuery($table, $builder, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            // todo: format json error
            $message = explode("\n", $ex->getMessage());
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param              $table
     * @param Builder      $builder
     * @param array        $extras
     * @return int|array
     * @throws BadRequestException
     * @throws InternalServerErrorException
     * @throws NotFoundException
     * @throws RestException
     */
    protected function runQuery($table, Builder $builder, $extras)
    {
        $schema = $this->parent->getTableSchema($table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $limit = intval(Arr::get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(Arr::get($extras, ApiOptions::OFFSET, 0));
        $countOnly = array_get_bool($extras, ApiOptions::COUNT_ONLY);
        $includeCount = array_get_bool($extras, ApiOptions::INCLUDE_COUNT);

        $maxAllowed = $this->getMaxRecordsReturnedLimit();
        $needLimit = false;
        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }

        // count total records
        $count = 0;
        if ($countOnly || $includeCount || $needLimit) {
            $count = $builder->count([DB::raw('1')]);
        }

        if ($countOnly) {
            return $count;
        }

        // apply the selected fields
        $select = $this->parseSelect($schema, $extras);
        $builder->select($select);

        // apply the rest of the parameters
        $order = trim(Arr::get($extras, ApiOptions::ORDER));
        if (!empty($order)) {
            if (false !== strpos($order, ';')) {
                throw new BadRequestException('Invalid order by clause in request.');
            }
            $commas = explode(',', $order);
            switch (count($commas)) {
                case 0:
                    break;
                case 1:
                    $spaces = explode(' ', $commas[0]);
                    $orderField = $spaces[0];
                    $direction = (isset($spaces[1]) ? $spaces[1] : 'asc');
                    $builder->orderBy($orderField, $direction);
                    break;
                default:
                    // todo need to validate format here first
                    $builder->orderByRaw($order);
                    break;
            }
        }
        $group = trim(Arr::get($extras, ApiOptions::GROUP));
        if (!empty($group)) {
            $group = static::fieldsToArray($group);
            $groups = $this->parseGroupBy($schema, $group);
            $builder->groupBy($groups);
        }
        $builder->take($limit);
        $builder->skip($offset);

        $result = $this->getQueryResults($schema, $builder, $extras);

        $meta = [];
        if ($includeCount || $needLimit) {
            if ($includeCount || $count > $maxAllowed) {
                $meta['count'] = $count;
            }
            if (($count - $offset) > $limit) {
                $meta['next'] = $offset + $limit;
            }
        }

        if (array_get_bool($extras, ApiOptions::INCLUDE_SCHEMA)) {
            try {
                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

        $data = $result->toArray();
        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }

    /**
     * @param TableSchema $schema
     * @param Builder     $builder
     * @param array       $extras
     * @return Collection
     */
    protected function getQueryResults(TableSchema $schema, Builder $builder, $extras)
    {
        $result = $builder->get();

        $result->transform(function ($item) use ($schema) {
            $item = (array)$item;
            foreach ($item as $field => &$value) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $value = $this->parent->getSchema()->typecastToClient($value, $fieldInfo);
                }
            }

            return $item;
        });

        return $result;
    }

    /**
     * @param ColumnSchema $field
     *
     * @return \Illuminate\Database\Query\Expression|string
     */
    protected function parseFieldForSelect($field)
    {
        if ($function = $field->getDbFunction(DbFunctionUses::SELECT)) {
            return $this->parent->getConnection()->raw($function . ' AS ' . $field->getName(true, true));
        }

        $out = $field->name;
        if (!empty($field->alias)) {
            $out .= ' AS ' . $field->alias;
        }

        return $out;
    }


    /**
     * @param  TableSchema $schema
     * @param  array|null  $extras
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseSelect($schema, $extras)
    {
        $fields = Arr::get($extras, ApiOptions::FIELDS);
        if (empty($fields)) {
            // minimally return the id fields
            $fields = Arr::get($extras, ApiOptions::ID_FIELD);
            if (empty($fields)) {
                $fields = $schema->getPrimaryKey();
                // if still nothing, return everything
                if (empty($fields)) {
                    $fields = ApiOptions::FIELDS_ALL;
                }
            }
        }
        $outArray = [];
        if (ApiOptions::FIELDS_ALL === $fields) {
            foreach ($schema->getColumns() as $fieldInfo) {
                if ($fieldInfo->isAggregate) {
                    continue;
                }
                $out = $this->parseFieldForSelect($fieldInfo);
                if (is_array($out)) {
                    $outArray = array_merge($outArray, $out);
                } else {
                    $outArray[] = $out;
                }
            }
        } else {
            $fields = static::fieldsToArray($fields);
            $related = Arr::get($extras, ApiOptions::RELATED);
            $allRelated = ('*' === $related);
            $related = static::fieldsToArray($related);
            if ($allRelated || !empty($related) || $schema->fetchRequiresRelations) {
                // add any required relationship mapping fields
                foreach ($schema->getRelations() as $relation) {
                    if ($relation->alwaysFetch || $allRelated || array_key_exists($relation->getName(true), $related)) {
                        foreach ($relation->field as $relField) {
                            if ($fieldInfo = $schema->getColumn($relField)) {
                                $relationField = $fieldInfo->getName(true); // account for aliasing
                                if (false === array_search($relationField, $fields)) {
                                    $fields[] = $relationField;
                                }
                            }
                        }
                    }
                }
            }
            $hasGroup = !empty(Arr::get($extras, ApiOptions::GROUP));
            foreach ($fields as $field) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $out = $this->parseFieldForSelect($fieldInfo);
                    if (is_array($out)) {
                        $outArray = array_merge($outArray, $out);
                    } else {
                        $outArray[] = $out;
                    }
                } elseif ($hasGroup && ($aggExpr = $this->parseAggregateExpression($schema, $field))) {
                    $outArray[] = $aggExpr;
                } else {
                    throw new BadRequestException('Invalid field requested: ' . $field);
                }
            }
        }

        return empty($outArray) ? ['*'] : $outArray;
    }

    /**
     * Split a comma-delimited fields string into an array. Ported from df-sqldb;
     * the modern df-database base no longer provides this to the connector.
     */
    protected static function fieldsToArray($fields)
    {
        if (empty($fields) || (ApiOptions::FIELDS_ALL === $fields)) {
            return [];
        }

        return (!is_array($fields)) ? array_map('trim', explode(',', trim($fields, ','))) : $fields;
    }

    /**
     * Validate group-by fields against the schema. Ported from df-sqldb.
     *
     * @param TableSchema $schema
     * @param array       $fields
     * @return array
     * @throws BadRequestException
     */
    protected function parseGroupBy($schema, $fields = null)
    {
        $outArray = [];
        if (!empty($fields)) {
            foreach ($fields as $field) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $outArray[] = $fieldInfo->name;
                } else {
                    throw new BadRequestException("Invalid or unknown field '$field' in group by clause.");
                }
            }
        }

        return $outArray;
    }

    /**
     * Parse an ad-hoc aggregate expression like SUM(column) or COUNT(*). Only allowed
     * when GROUP BY is present; validates the inner column against the schema to block
     * injection. Ported from df-sqldb.
     *
     * @param TableSchema $schema
     * @param string      $field
     * @return \Illuminate\Database\Query\Expression|null
     */
    protected function parseAggregateExpression($schema, $field)
    {
        $allowed = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'];
        $pattern = '/^(' . implode('|', $allowed) . ')\s*\(\s*(.+?)\s*\)$/i';

        if (!preg_match($pattern, trim($field), $matches)) {
            return null;
        }

        $func = strtoupper($matches[1]);
        $inner = $matches[2];

        if ($func === 'COUNT' && $inner === '*') {
            return DB::raw('COUNT(*) AS COUNT_ALL');
        }

        // Reject sub-expressions / injection attempts
        if (preg_match('/[;\'"\(\)\\\\]/', $inner)) {
            return null;
        }

        $columnInfo = $schema->getColumn($inner, true);
        if (!$columnInfo) {
            return null;
        }

        $columnName = $columnInfo->name;
        $alias = $func . '_' . preg_replace('/[^a-zA-Z0-9_]/', '_', $columnInfo->getName(true));

        return DB::raw($func . '(' . $this->parent->getConnection()->getQueryGrammar()->wrap($columnName) . ') AS ' . $alias);
    }

    /**
     * Convert a single filter value into a bound placeholder. Ported from df-sqldb;
     * without it every filtered query fatals with "undefined method parseFilterValue".
     *
     * @param mixed        $value
     * @param ColumnSchema $info
     * @param array        $out_params
     * @param array        $in_params
     * @return string
     */
    protected function parseFilterValue($value, ColumnSchema $info, array &$out_params, array $in_params = [])
    {
        // if a named replacement parameter, un-name it because Laravel can't handle named parameters
        if (is_array($in_params) && (0 === strpos($value, ':'))) {
            if (array_key_exists($value, $in_params)) {
                $value = $in_params[$value];
            }
        }

        // remove quoting on strings if used, i.e. 1.x required them
        if (is_string($value)) {
            if ((0 === strcmp("'" . trim($value, "'") . "'", $value)) ||
                (0 === strcmp('"' . trim($value, '"') . '"', $value))
            ) {
                $value = substr($value, 1, -1);
            } elseif ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }

        // anything else schema specific
        $value = $this->parent->getSchema()->typecastToNative($value, $info);

        $out_params[] = $value;
        $value = '?';

        return $value;
    }


    /**
     * Take in a ANSI SQL filter string (WHERE clause)
     * or our generic NoSQL filter array or partial record
     * and parse it to the service's native filter criteria.
     * The filter string can have substitution parameters such as
     * ':name', in which case an associative array is expected,
     * for value substitution.
     *
     * @param \Illuminate\Database\Query\Builder $builder
     * @param string | array                     $filter       SQL WHERE clause filter string
     * @param array                              $params       Array of substitution values
     * @param array                              $ss_filters   Server-side filters to apply
     * @param array                              $avail_fields All available fields for the table
     *
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function convertFilterToNative(
        Builder $builder,
                $filter,
                $params = [],
                $ss_filters = [],
                $avail_fields = []
    ) {
        // interpret any parameter values as lookups
        $params = (is_array($params) ? static::interpretRecordValues($params) : []);
        $serverFilter = $this->buildQueryStringFromData($ss_filters);

        $outParams = [];
        if (empty($filter)) {
            $filter = $serverFilter;
        } elseif (is_string($filter)) {
            if (!empty($serverFilter)) {
                $filter = '(' . $filter . ') ' . DbLogicalOperators::AND_STR . ' (' . $serverFilter . ')';
            }
        } elseif (is_array($filter)) {
            // todo parse client filter?
            $filter = '';
            if (!empty($serverFilter)) {
                $filter = '(' . $filter . ') ' . DbLogicalOperators::AND_STR . ' (' . $serverFilter . ')';
            }
        }

        Session::replaceLookups($filter);
        $filterString = $this->parseFilterString($filter, $outParams, $avail_fields, $params);
        if (!empty($filterString)) {
            $builder->whereRaw($filterString, $outParams);
        }
    }

    /**
     * @param       $filter_info
     *
     * @return null|string
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function buildQueryStringFromData($filter_info)
    {
        $filters = Arr::get($filter_info, 'filters');
        if (empty($filters)) {
            return null;
        }

        $sql = '';
        $combiner = Arr::get($filter_info, 'filter_op', DbLogicalOperators::AND_STR);
        foreach ($filters as $key => $filter) {
            if (!empty($sql)) {
                $sql .= " $combiner ";
            }

            $name = Arr::get($filter, 'name');
            $op = strtoupper(Arr::get($filter, 'operator'));
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            if (DbComparisonOperators::requiresNoValue($op)) {
                $sql .= "($name $op)";
            } else {
                $value = Arr::get($filter, 'value');
                $sql .= "($name $op $value)";
            }
        }

        return $sql;
    }

    /**
     * @param string         $filter
     * @param array          $out_params
     * @param ColumnSchema[] $fields_info
     * @param array          $in_params
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($filter, array &$out_params, $fields_info, array $in_params = [])
    {
        if (empty($filter)) {
            return null;
        }

        $filter = trim($filter);
        // todo use smarter regex
        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1) or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . ' (')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = $this->parseFilterString($parts, $out_params, $fields_info, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($left, $out_params, $fields_info, $in_params);
                    $right = $this->parseFilterString($right, $out_params, $fields_info, $in_params);

                    return $left . ' ' . static::localizeOperator($logicalOp) . ' ' . $right;
                }
            }
        }

        $wrap = false;
        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
            // remove unnecessary wrapping ()
            $filter = substr($filter, 1, -1);
            $wrap = true;
        }

        // Some scenarios leave extra parens dangling
        $pure = trim($filter, '()');
        $pieces = explode($pure, $filter);
        $leftParen = (!empty($pieces[0]) ? $pieces[0] : null);
        $rightParen = (!empty($pieces[1]) ? $pieces[1] : null);
        $filter = $pure;

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    $partsCount = count($parts);
                    if (($partsCount > 1) &&
                        (0 === strcasecmp($parts[$partsCount - 1], trim(DbLogicalOperators::NOT_STR)))
                    ) {
                        // negation on left side of operator
                        array_pop($parts);
                        $field = implode(' ', $parts);
                        $negate = true;
                    }
                }
                /** @type ColumnSchema $info */
                if (null === $info = Arr::get($fields_info, strtolower($field))) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if ((0 !== strpos($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                        // remove wrapping ()
                        $value = substr($value, 1, -1);
                        $parsed = [];
                        foreach (explode(',', $value) as $each) {
                            $parsed[] = $this->parseFilterValue(trim($each), $info, $out_params, $in_params);
                        }
                        $value = '(' . implode(',', $parsed) . ')';
                    } else {
                        throw new BadRequestException('Filter value lists must be wrapped in parentheses.');
                    }
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    $value = null;
                } else {
                    static::modifyValueByOperator($sqlOp, $value);
                    $value = $this->parseFilterValue($value, $info, $out_params, $in_params);
                }

                $sqlOp = static::localizeOperator($sqlOp);
                if ($negate) {
                    $sqlOp = DbLogicalOperators::NOT_STR . ' ' . $sqlOp;
                }

                if ($function = $info->getDbFunction(DbFunctionUses::FILTER)) {
                    $out = $this->parent->getConnection()->raw($function);
                } else {
                    $out = $info->quotedName;
                }
                $out .= " $sqlOp";
                $out .= (isset($value) ? " $value" : null);
                if ($leftParen) {
                    $out = $leftParen . $out;
                }
                if ($rightParen) {
                    $out .= $rightParen;
                }

                return ($wrap ? '(' . $out . ')' : $out);
            }
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }



    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }
}