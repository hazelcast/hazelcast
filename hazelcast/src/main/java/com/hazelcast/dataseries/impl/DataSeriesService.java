package com.hazelcast.dataseries.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class DataSeriesService implements ManagedService, RemoteService {
    public static final String SERVICE_NAME = "hz:impl:dataSeriesService";
    private final ConcurrentMap<String, DataSeriesContainer> containers = new ConcurrentHashMap<String, DataSeriesContainer>();

    private final ConstructorFunction<String, DataSeriesContainer> containerConstructorFunction =
            new ConstructorFunction<String, DataSeriesContainer>() {
                public DataSeriesContainer createNew(String key) {
                    Config config = nodeEngine.getConfig();
                    DataSeriesConfig dataSeriesConfig = config.findDataSeriesConfig(key);
                    return new DataSeriesContainer(dataSeriesConfig, nodeEngine);
                }
            };

    private NodeEngineImpl nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    public DataSeriesContainer getDataSeriesContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    public DataSeriesContainer getDataSeriesContainer(String name, final DataSeriesConfig config) {
        return getOrPutIfAbsent(containers, name, key -> new DataSeriesContainer(config, nodeEngine));
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new DataSeriesProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }
}
