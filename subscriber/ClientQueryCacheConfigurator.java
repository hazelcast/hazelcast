package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.impl.querycache.subscriber.AbstractQueryCacheConfigurator;

import java.util.HashMap;
import java.util.Map;

/**
 * Client side implementation of {@link QueryCacheConfigurator}
 *
 * @see QueryCacheConfigurator
 */
public class ClientQueryCacheConfigurator extends AbstractQueryCacheConfigurator {

    private final ClientConfig clientConfig;

    public ClientQueryCacheConfigurator(ClientConfig clientConfig,
                                        QueryCacheEventService eventService) {
        super(clientConfig.getClassLoader(), eventService);
        this.clientConfig = clientConfig;
    }

    @Override
    public QueryCacheConfig getOrCreateConfiguration(String mapName, String cacheName) {
        Map<String, Map<String, QueryCacheConfig>> allQueryCacheConfig = clientConfig.getQueryCacheConfigs();

        Map<String, QueryCacheConfig> mapQueryCacheConfig = allQueryCacheConfig.get(mapName);
        if (mapQueryCacheConfig == null) {
            mapQueryCacheConfig = new HashMap<String, QueryCacheConfig>();
            allQueryCacheConfig.put(mapName, mapQueryCacheConfig);
        }

        QueryCacheConfig config = mapQueryCacheConfig.get(cacheName);
        if (config == null) {
            config = new QueryCacheConfig(cacheName);
            mapQueryCacheConfig.put(cacheName, config);
        }

        setPredicateImpl(config);
        setEntryListener(mapName, cacheName, config);

        return config;
    }

    @Override
    public QueryCacheConfig getOrNull(String mapName, String cacheName) {
        Map<String, Map<String, QueryCacheConfig>> allQueryCacheConfig = clientConfig.getQueryCacheConfigs();

        Map<String, QueryCacheConfig> mapQueryCacheConfig = allQueryCacheConfig.get(mapName);
        if (mapQueryCacheConfig == null) {
            return null;
        }

        return mapQueryCacheConfig.get(cacheName);
    }

    @Override
    public void removeConfiguration(String mapName, String cacheName) {
        Map<String, Map<String, QueryCacheConfig>> allQueryCacheConfig = clientConfig.getQueryCacheConfigs();
        Map<String, QueryCacheConfig> mapQueryCacheConfig = allQueryCacheConfig.get(mapName);
        if (mapQueryCacheConfig == null || mapQueryCacheConfig.isEmpty()) {
            return;
        }
        mapQueryCacheConfig.remove(cacheName);
    }

}
