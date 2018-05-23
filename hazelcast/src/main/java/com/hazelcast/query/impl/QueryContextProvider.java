package com.hazelcast.query.impl;

/**
 * Provides query contexts for {@link Indexes} to execute queries.
 */
public interface QueryContextProvider {

    /**
     * Obtains a query context for the given indexes.
     * <p>
     * The returned query context instance is valid for a duration of a single
     * query and should be re-obtained for every query.
     *
     * @param indexes the indexes to obtain the query context for.
     * @return the obtained query context.
     */
    QueryContext obtainContextFor(Indexes indexes);

}
