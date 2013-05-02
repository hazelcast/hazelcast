package com.hazelcast.nio.serialization;

/**
 * @mdogan 4/30/13
 */
public final class FactoryIdHelper {

    public static final String SPI_DS_FACTORY = "hazelcast.serialization.ds.spi";
    public static final String CLUSTER_DS_FACTORY = "hazelcast.serialization.ds.cluster";
    public static final String MAP_DS_FACTORY = "hazelcast.serialization.ds.map";
    public static final String QUEUE_DS_FACTORY = "hazelcast.serialization.ds.queue";
    public static final String COLLECTION_DS_FACTORY = "hazelcast.serialization.ds.collection";
    public static final String EXECUTOR_DS_FACTORY = "hazelcast.serialization.ds.executor";

    public static final String SPI_PORTABLE_FACTORY = "hazelcast.serialization.portable.spi";
    public static final String CLUSTER_PORTABLE_FACTORY = "hazelcast.serialization.portable.cluster";
    public static final String CLIENT_PORTABLE_FACTORY = "hazelcast.serialization.portable.client";
    public static final String MAP_PORTABLE_FACTORY = "hazelcast.serialization.portable.map";
    public static final String QUEUE_PORTABLE_FACTORY = "hazelcast.serialization.portable.queue";
    public static final String COLLECTION_PORTABLE_FACTORY = "hazelcast.serialization.portable.collection";
    public static final String EXECUTOR_PORTABLE_FACTORY = "hazelcast.serialization.portable.executor";


    public static int getFactoryId(String prop, int defaultId) {
        final String value = System.getProperty(prop);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultId;
    }

}
