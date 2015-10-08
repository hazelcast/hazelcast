package com.hazelcast.query.extractor;

public abstract class ValueExtractor {

    /**
     * No-arg constructor required for the runtime instantiation of the extractor
     */
    public ValueExtractor() {
    }

    /**
     * Extracts a value from the given target object.
     * <p/>
     * May return:
     * <ul>
     * <li>a single single-value result (not a collection)</li>
     * <li>a single multi-value result (a collection)</li>
     * <li>multiple single-value or multi-value results (@see com.hazelcast.query.extractor.MultiResult)</li>
     * </ul>
     * <p/>
     * MultiResult is an aggregate of results that is returned if the extractor returns multiple results
     * due to a reduce operation executed on a hierarchy of values.
     *
     * @param target object to extract the value from
     * @return extracted value
     */
    public abstract Object extract(Object target);

}
