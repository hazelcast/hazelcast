package com.hazelcast.web;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.Predicate;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HazelcastInstanceDelegate {

    protected static final ILogger LOGGER = Logger.getLogger(HazelcastInstanceDelegate.class);

    private final HazelcastInstance instance;

    public HazelcastInstanceDelegate(HazelcastInstance instance) {
        this.instance = instance;
    }

    public HazelcastInstanceDelegate(FilterConfig filterConfig, Properties properties) {
        HazelcastInstance instance;
        try {
           instance =  HazelcastInstanceLoader.createInstance(filterConfig, properties);
        } catch (ServletException e) {
            instance = null;
        }
        this.instance = instance;
    }

    public Config getInstanceConfig() {
        return instance.getConfig();
    }

    public void addEntryListener(String mapName, EntryListener listener, boolean includeValue){
        try {
            getClusterMap(mapName).addEntryListener(listener, includeValue);
        } catch (Exception e) {
            LOGGER.warning("An exception occured while creating entry listener");
        }
    }

    public IMap<String, Object> getClusterMap(String mapName) {
        return instance.getMap(mapName);
    }

    public Object get(String mapName, String key) {
        try {
            return getClusterMap(mapName).get(key);
        } catch (Exception e) {
            LOGGER.warning("An exception occured while getting item with key" + key);
            return null;
        }
    }

    public void set(String mapName, String key, Object value) {
        try {
            getClusterMap(mapName).set(key, value);
        } catch (Exception e) {
            LOGGER.warning("An exception occured while setting item with key" + key);
        }
    }

    public void delete(String mapName, String key) {
        try {
            getClusterMap(mapName).delete(key);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during processing item with key" + key);
        }
    }

    public Set<Map.Entry<String, Object>> entrySet(String mapName, Predicate predicate) {
        try {
            return getClusterMap(mapName).entrySet(predicate);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during query with predicate." + predicate);
            return null;
        }
    }

    public Set<String> keySet(String mapName, Predicate predicate) {
        try {
            return getClusterMap(mapName).keySet(predicate);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during fetching key set with predicate." + predicate);
            return null;
        }
    }


    public Object executeOnKey(String mapName, String key, AbstractWebDataEntryProcessor webDataEntryProcessor) {
        try {
            return getClusterMap(mapName).executeOnKey(key, webDataEntryProcessor);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during processing item with key" + key);
            return null;
        }
    }

    public Object executeOnEntries(String mapName, AbstractWebDataEntryProcessor webDataEntryProcessor) {
        try {
            return getClusterMap(mapName).executeOnEntries(webDataEntryProcessor);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during processing all entries.");
            return null;
        }
    }

    public void shutdown() {
        try {
            this.instance.getLifecycleService().shutdown();
        } catch (Exception e) {
            LOGGER.warning("An exception occured during shutdown of lifecycle service.");
        }
    }


}
