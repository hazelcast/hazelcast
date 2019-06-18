package com.hazelcast.config;

// TODO: Pass number of threads to member attribute on start?
public class QueryConfig {

    public static final int DFLT_CONTROL_THREAD_COUNT = 2;
    public static final int DFLT_DATA_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private int controlThreadCount = DFLT_CONTROL_THREAD_COUNT;
    private int dataThreadCount = DFLT_DATA_THREAD_COUNT;

    public int getControlThreadCount() {
        return controlThreadCount;
    }

    public QueryConfig setControlThreadCount(int controlThreadCount) {
        this.controlThreadCount = controlThreadCount;
        return this;
    }

    public int getDataThreadCount() {
        return dataThreadCount;
    }

    public QueryConfig setDataThreadCount(int dataThreadCount) {
        this.dataThreadCount = dataThreadCount;
        return this;
    }

    @Override
    public String toString() {
        return "QueryConfig{controlThreadCount=" + controlThreadCount + ", dataThreadCount=" + dataThreadCount + '}';
    }
}
