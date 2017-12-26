package com.hazelcast.dataset;


import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.dataset.impl.MemoryInfo;
import com.hazelcast.query.Predicate;

/**
 *
 * broken:
 * - expiration of segments
 * - queries: indexes queries only work for query, not for the others like projection, aggregation
 *
 * todo:
 * - primary index
 * - offloading
 * - analyzing compilation so multiple members can share the same compilation
 * - parallel processing of a single partition (could easily be done on segment level).
 * - option to not partition the data. E.g. when the amount of data is relatively small.
 *      - in theory this can be done by using single partition key; problem is with e.g. count etc. This will force
 *      all the segments to be made
 * - option to store the 'insert' time in the record.
 * - the insert could return a 'sequence-id' this can be used to read the record.
 * - frequency distribution aggregation
 * - column store:
 *      - so if a single column is used; the data in the column could be compressed. E.g. if there are 20 zero's
 *      instead of having a single row per 'record', it states 'now 20 zero's'
 * - queries
 *      - more advanced query analysis.
 *      - or queries
 * - mappings
 *       - primitive wrappers (so nullable fields)
 *       - enum fields
 *       - there could be a set of bytes added to each record where a bit is allocated per nullable field. So 8 nullable fields,
 *       can share 1 byte. 9 nullable fields, require 2 bytes.
 *       - string fields
 * - predicates
 *      - for all predicate
 *      - exists predicate
 *      - regular predicates using the domain object
 *      - in predicate
 *      - partition predicate
 *      - between predicate
 *      - limited time query: allow certain operations on a segment. For example a query should be allowed to be executed
 *        on 8/9h, even though the time currently might be 13h.
 *
 * done:
 * - limit on the max number of segments kept in memory
 * - projection: ability to specify collection to return so that duplication removal can be done at the source.
 * - projection isn't returning a result.
 * - precalculation: e.g. on a completed segment (where no data can change), certain calculations could be cached. E.g. the
 *      min/max/sum etc can be cached. No need to recalculate them again and again
 * - full 'entry processors' that are able to modify the whole record
 * - secondary index
 * - conditional entry processors
 * - entry processors
 * - make a dataset from an IMap
 * - growing of the data-set
 * - char fields
 * - projection
 * - aggregation
 * - queries are not returning a result.
 * - aggregation/queries can return a new dataset that isn't retrieved but stored remotely.
 *
 *
 * <h1>Secondary index</h1>
 *
 * <h1>Growing</h1>
 *
 * <h2>Infinite</h2>
 * Infinite segment capacity or infinite number of segments with some fixed capacity?
 *
 * Currently the DataSet can grow indefinitely. Currently we have this one.
 *
 * <h2>Bound</h2>
 * There should be an option for a maximum capacity.
 *
 * <h2>Time based</h2>
 * Writes in a single time 'group' get written to some segment and all the time groups together form to total retention.
 * E.g. if there is a 48h retention, one could create 48 1h segments. Old segments gets dropped, and new segments are created
 * when writes are done.
 *
 * <h3>Ring</h3>
 * So overwrite older segments.
 *
 * <h1>String</h1>
 * a string can be fixed max length. So if it is defined as 20 chars, then 20 chars storage is allocated. If only 5 chars are used,
 * the remaining 15 chars are zero'd.
 *
 * variable length strings:
 * These are very difficult to deal with in case of a fixed length record.
 *
 * String should also have option to indicate nullability.
 *
 * <h1>More complex mappings.</h1>
 * - mapping of string; the actual byte content could be written directly to offheap. But it requires a fixed size mapping.
 * - mapping of enums
 * - mappings of nullable primitive wrappers
 *
 * So instead of doing an actual mem-copy; write the fields one by one. And each field
 * knows what it should do. E.g. in case of a primitive wrapper, either the null bit is set, or the null bit is unset and the
 * value is set.
 *
 * @param <K>
 * @param <V>
 */
public interface DataSet<K, V> extends DistributedObject {

    void insert(K partitionKey, V value);

    void populate(IMap<K, V> map);

    ICompletableFuture insertAsync(K partitionKey, V value);

    CompiledEntryProcessor compile(EntryProcessorRecipe recipe);

    CompiledPredicate<V> compile(Predicate predicate);

    <E> E aggregate(String aggregatorId);

    <E> CompiledProjection<K,E> compile(ProjectionRecipe<E> recipe);

    <T, E> CompiledAggregation<E> compile(AggregationRecipe<T, E> recipe);

    /**
     * Returns the number of records in this set.
     *
     * @return number of items in this set.
     */
    long count();

    MemoryInfo memoryUsage();
}
