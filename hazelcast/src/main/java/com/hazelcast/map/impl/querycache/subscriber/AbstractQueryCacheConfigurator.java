/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.EventListener;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Abstract {@link QueryCacheConfigurator} includes common functionality.
 *
 * @see QueryCacheConfigurator
 */
public abstract class AbstractQueryCacheConfigurator implements QueryCacheConfigurator {

    private final ClassLoader configClassLoader;
    private final QueryCacheEventService eventService;

    public AbstractQueryCacheConfigurator(ClassLoader configClassLoader, QueryCacheEventService eventService) {
        this.configClassLoader = configClassLoader;
        this.eventService = eventService;
    }

    protected void setEntryListener(String mapName, String cacheId, QueryCacheConfig config) {
        for (EntryListenerConfig listenerConfig : config.getEntryListenerConfigs()) {
            MapListener listener = getListener(listenerConfig);
            if (listener != null) {
                EventFilter filter = new EntryEventFilter(null, listenerConfig.isIncludeValue());
                eventService.addListener(mapName, cacheId, listener, filter);
            }
        }
    }

    protected void setPredicateImpl(QueryCacheConfig config) {
        PredicateConfig predicateConfig = config.getPredicateConfig();
        if (predicateConfig.getImplementation() != null) {
            return;
        }
        Predicate predicate = getPredicate(predicateConfig);
        if (predicate == null) {
            return;
        }
        predicateConfig.setImplementation(predicate);
    }

    private Predicate getPredicate(PredicateConfig predicateConfig) {

        if (!isNullOrEmpty(predicateConfig.getClassName())) {
            try {
                return ClassLoaderUtil
                        .newInstance(configClassLoader, predicateConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        if (!isNullOrEmpty(predicateConfig.getSql())) {
            String sql = predicateConfig.getSql();
            return Predicates.sql(sql);
        }

        return null;
    }

    private <T extends EventListener> T getListener(ListenerConfig listenerConfig) {
        T listener = null;
        if (listenerConfig.getImplementation() != null) {
            listener = (T) listenerConfig.getImplementation();
        } else if (listenerConfig.getClassName() != null) {
            try {
                return ClassLoaderUtil
                        .newInstance(configClassLoader, listenerConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return listener;
    }
}
