/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.web;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.web.spring.SessionCleanUpService;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class HazelcastInstanceDelegate {

    protected static final ILogger LOGGER = Logger.getLogger(HazelcastInstanceDelegate.class);

    private final HazelcastInstance instance;
    private final SessionCleanUpService cleanUpService;

    public HazelcastInstanceDelegate(FilterConfig filterConfig, Properties properties) {
        HazelcastInstance instance;
        try {
           instance =  HazelcastInstanceLoader.createInstance(filterConfig, properties);
        } catch (ServletException e) {
            instance = null;
        }
        this.instance = instance;
        cleanUpService = new SessionCleanUpService(instance.getName());
        cleanUpService.registerHazelcastInstanceDelegate(this);
    }

    public Config getInstanceConfig() {
        return instance.getConfig();
    }

    public void addEntryListener(String mapName, EntryListener listener, boolean includeValue) {
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
            LOGGER.warning("An exception occured while getting item with key" + key, e);
            return null;
        }
    }

    public void set(String mapName, String key, Object value) {
        try {
            getClusterMap(mapName).set(key, value);
        } catch (Exception e) {
            LOGGER.warning("An exception occured while setting item with key" + key, e);
        }
    }

    public void delete(String mapName, String key) {
        try {
            getClusterMap(mapName).delete(key);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during processing item with key" + key , e);
        }
    }

    public Set<Map.Entry<String, Object>> entrySet(String mapName, Predicate predicate) {
        try {
            return getClusterMap(mapName).entrySet(predicate);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during query with predicate." + predicate, e);
            return null;
        }
    }

    public Set<String> keySet(String mapName, Predicate predicate) {
        try {
            return getClusterMap(mapName).keySet(predicate);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during fetching key set with predicate." + predicate , e);
            return null;
        }
    }


    public Object executeOnKey(String mapName, String key, EntryProcessor entryProcessor) {
        try {
            return getClusterMap(mapName).executeOnKey(key, entryProcessor);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during processing item with key" + key , e);
            cleanUpService.putKeyEntryProcessorPairToFailQueue(mapName, key,
                    entryProcessor);
            return null;
        }
    }

    public Object executeOnEntries(String mapName, EntryProcessor entryProcessor) {
        try {
            return getClusterMap(mapName).executeOnEntries(entryProcessor);
        } catch (Exception e) {
            LOGGER.warning("An exception occured during processing all entries." , e);
            return null;
        }
    }

    public void shutdown() {
        try {
            this.instance.getLifecycleService().shutdown();
        } catch (Exception e) {
            LOGGER.warning("An exception occured during shutdown of lifecycle service.", e);
        }
    }
}
