<?php
namespace DreamFactory\Core\BigQuery\Resources;

use DB;
use DreamFactory\Core\Database\Resources\BaseDbTableResource;
use DreamFactory\Core\SqlDb\Resources\Table as MySqlTable;

class Table extends BaseDbTableResource
{

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        return [];
        $dbConn = $this->parent->getConnection();
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            if (0 < $dbConn->transactionLevel()) {
                $dbConn->commit();
            }

            return null;
        }

        $updates = array_get($extras, 'updates');
        $ssFilters = array_get($extras, 'ss_filters');
        $related = array_get($extras, 'related');
        $requireMore = array_get_bool($extras, 'require_more') || !empty($related);

        $builder = $dbConn->table($this->transactionTableSchema->internalName);

        /** @type ColumnSchema $idName */
        $idName = (isset($this->tableIdsInfo, $this->tableIdsInfo[0])) ? $this->tableIdsInfo[0] : null;
        if (empty($idName)) {
            throw new BadRequestException('No valid identifier found for this table.');
        }

        if (!empty($this->batchRecords)) {
            if (is_array($this->batchRecords[0])) {
                $temp = [];
                foreach ($this->batchRecords as $record) {
                    $temp[] = array_get($record, $idName->getName(true));
                }

                $builder->whereIn($idName->name, $temp);
            } else {
                $builder->whereIn($idName->name, $this->batchRecords);
            }
        } else {
            $builder->whereIn($idName->name, $this->batchIds);
        }

        $serverFilter = $this->buildQueryStringFromData($ssFilters);
        if (!empty($serverFilter)) {
            Session::replaceLookups($serverFilter);
            $params = [];
            $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
            $builder->whereRaw($filterString, $params);
        }

        $out = [];
        $action = $this->getAction();
        if (!empty($this->batchRecords)) {
            if (1 == count($this->tableIdsInfo)) {
                // records are used to retrieve extras
                // ids array are now more like records
                $result = $this->runQuery($this->transactionTable, $builder, $extras);
                if (empty($result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                $out = $result;
            } else {
                $out = $this->retrieveRecords($this->transactionTable, $this->batchRecords, $extras);
            }

            $this->batchRecords = [];
        } elseif (!empty($this->batchIds)) {
            switch ($action) {
                case Verbs::PUT:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $builder->update($parsed);
                            if (count($this->batchIds) !== $rows) {
                                throw new BadRequestException('Batch Error: Not all requested records could be updated.');
                            }
                        }


                        if ($requireMore) {
                            $result = $this->runQuery(
                                $this->transactionTable,
                                $builder,
                                $extras
                            );

                            $out = $result;
                        }
                    }
                    break;

                case Verbs::DELETE:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );
                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }
                    } else {
                        $out = $result;
                    }

                    $rows = $builder->delete();
                    if (count($this->batchIds) !== $rows) {
                        throw new BatchException($out, 'Batch Error: Not all requested records could be deleted.');
                    }
                    break;

                case Verbs::GET:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );

                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }

                        throw new BatchException($out, 'Batch Error: Not all requested records could be retrieved.');
                    }

                    $out = $result;
                    break;

                default:
                    break;
            }

            if (empty($out)) {
                $out = [];
                foreach ($this->batchIds as $id) {
                    $out[] = [$idName->getName(true) => $id];
                }
            }

            $this->batchIds = [];
        }

        if (0 < $dbConn->transactionLevel()) {
            $dbConn->commit();
        }

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
        return [];
        $idsInfo = [];
        if (empty($requested_fields)) {
            $requested_fields = [];
            /** @type ColumnSchema[] $idsInfo */
            $idsInfo = static::getPrimaryKeys($fields_info);
            foreach ($idsInfo as $info) {
                $requested_fields[] = $info->getName(true);
            }
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
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        return [];
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            $tableSchema = $this->parent->getTableSchema($table);
            if (!$tableSchema) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }

            $fieldsInfo = $tableSchema->getColumns(true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            return $this->runQuery($table, $builder, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }
}