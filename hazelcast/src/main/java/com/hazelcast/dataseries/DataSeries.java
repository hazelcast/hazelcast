package com.hazelcast.dataseries;


import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.function.Supplier;

import java.util.Iterator;

/**
 * broken:
 * - expiration of segments
 * - queries: indexes queries only work for query, not for the others like projection, aggregation
 *
 * todo:
 * - fork join option for queries?
 * - primary index
 * - analyzing compilation so multiple members can share the same compilation
 * - option to not partition the data. E.g. when the amount of data is relatively small.
 * - in theory this can be done by using single partition key; problem is with e.g. count etc. This will force
 * all the segments to be made
 * - option to store the 'insert' time in the record.
 * - the insert could return a 'sequence-id' this can be used to read the record.
 * - frequency distribution aggregation
 * - column store:
 * - so if a single column is used; the data in the column could be compressed. E.g. if there are 20 zero's
 * instead of having a single row per 'record', it states 'now 20 zero's'
 * - queries
 *      - more advanced query analysis.
 *      - or queries
 * - mappings
 *      - primitive wrappers (so nullable fields)
 *      - enum fields
 *      - there could be a set of bytes added to each record where a bit is allocated per nullable field. So 8 nullable fields,
 *      can share 1 byte. 9 nullable fields, require 2 bytes.
 *      - string fields
 * - predicates
 *      - for all predicate
 *      - exists predicate
 *      - regular predicates using the domain object
 *      - in predicate
 *      - partition predicate
 *      - between predicate
 * - limited time query: allow certain operations on a segment. For example a query should be allowed to be executed
 * on 8/9h, even though the time currently might be 13h.
 *
 * done:
 * - aggregation: waiting on the completion of the tenuredsegments, is done on the partition thread. This isn't needed
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
public interface DataSeries<K, V> extends DistributedObject {

    void fill(long count, Supplier<V> supplier);

    void populate(IMap<K, V> src);

    void append(K partitionKey, V value);

    ICompletableFuture appendAsync(K partitionKey, V value);

    PreparedEntryProcessor prepare(EntryProcessorRecipe recipe);

    PreparedQuery<V> prepare(Predicate predicate);

    <E> PreparedProjection<K, E> prepare(ProjectionRecipe<E> recipe);

    <T, E> PreparedAggregation<E> prepare(AggregationRecipe<T, E> recipe);

    <E> E aggregate(String aggregatorId);

    /**
     * No more mutations (important to close the eden segment so no processing on partition thread)
     */
    void freeze();

    /**
     * Returns the number of records in this set.
     *
     * @return number of items in this set.
     */
    long count();

    MemoryInfo memoryInfo();

    MemoryInfo memoryInfo(int partitionId);
}
