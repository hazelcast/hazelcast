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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.web.entryprocessor.DeleteSessionEntryProcessor;
import com.hazelcast.web.entryprocessor.GetAttributeEntryProcessor;
import com.hazelcast.web.entryprocessor.GetAttributeNamesEntryProcessor;
import com.hazelcast.web.entryprocessor.GetSessionStateEntryProcessor;
import com.hazelcast.web.entryprocessor.SessionUpdateEntryProcessor;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 * ClusteredSessionService is a proxy class which delegates all
 * operations on distributed map to hazelcast cluster used for session
 * replication. ClusteredSessionService is also responsible for safety
 * of the cluster connection. If a map operation fails due to network
 * issued, a background thread always retries connection and ensures the
 * safety of connection.
 */
public class ClusteredSessionService {

    /**
     * The constant LOGGER.
     */
    protected static final ILogger LOGGER = Logger.getLogger(ClusteredSessionService.class);
    private static final long CLUSTER_CHECK_INTERVAL = 5L;
    private static final long RETRY_MILLIS = 7000;

    private volatile IMap clusterMap;
    private volatile SerializationServiceSupport sss;
    private volatile HazelcastInstance hazelcastInstance;

    private final FilterConfig filterConfig;
    private final Properties properties;
    private final String clusterMapName;

    private final Queue<AbstractMap.SimpleEntry<String, Boolean>> orphanSessions = new
            LinkedBlockingQueue<AbstractMap.SimpleEntry<String, Boolean>>();

    private volatile boolean failedConnection = true;
    private volatile long lastConnectionTry;
    private final ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor(new EnsureInstanceThreadFactory());

    /**
     * Instantiates a new Clustered session service.
     *
     * @param filterConfig   the filter config
     * @param properties     the properties
     * @param clusterMapName the cluster map name
     */
    public ClusteredSessionService(FilterConfig filterConfig, Properties properties, String clusterMapName) {
        this.filterConfig = filterConfig;
        this.properties = properties;
        this.clusterMapName = clusterMapName;
        try {
            init();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
    }

    public void setFailedConnection(boolean failedConnection) {
        this.failedConnection = failedConnection;
    }

    public void init() throws Exception {
        ensureInstance();
        es.scheduleWithFixedDelay(new Runnable() {
            public void run() {
            try {
                ensureInstance();
            } catch (Exception e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Cannot connect hazelcast server", e);
                }
            }
            }
        }, 2 * CLUSTER_CHECK_INTERVAL, CLUSTER_CHECK_INTERVAL, TimeUnit.SECONDS);
    }


    private void ensureInstance() throws Exception {
        if (failedConnection && System.currentTimeMillis() > lastConnectionTry + RETRY_MILLIS) {
            synchronized (this) {
                try {
                    if (failedConnection && System.currentTimeMillis() > lastConnectionTry + RETRY_MILLIS) {
                        reconnectHZInstance();
                        clearOrphanSessionQueue();
                    }
                } catch (Exception e) {
                    setFailedConnection(true);
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Cannot connect hazelcast server", e);
                    }
                }
            }
        }
    }

    private void reconnectHZInstance() throws ServletException {
        LOGGER.info("Retrying the connection!!");
        lastConnectionTry = System.currentTimeMillis();
        hazelcastInstance = HazelcastInstanceLoader.createInstance(this, filterConfig, properties, clusterMapName);
        clusterMap = hazelcastInstance.getMap(clusterMapName);
        sss = (SerializationServiceSupport) hazelcastInstance;
        setFailedConnection(false);
        LOGGER.info("Successfully Connected!");
    }

    private void clearOrphanSessionQueue() {
        AbstractMap.SimpleEntry<String, Boolean> sessionIdEntry = orphanSessions.poll();
        while (sessionIdEntry != null) {
            if (!deleteSession(sessionIdEntry.getKey(), sessionIdEntry.getValue())) {
                // do not continue
                sessionIdEntry = null;
            } else {
                sessionIdEntry = orphanSessions.poll();
            }
        }
    }

    /**
     * Execute on key.
     *
     * @param sessionId the session id
     * @param processor the processor
     * @return the object
     * @throws Exception
     */
    Object executeOnKey(String sessionId, EntryProcessor processor) throws Exception {
        try {
            return clusterMap.executeOnKey(sessionId, processor);
        } catch (Exception e) {
            LOGGER.finest("Cannot connect hazelcast server", e);
            throw e;
        }
    }

    /**
     * Gets attributes.
     *
     * @param sessionId the session id
     * @return the attributes
     * @throws Exception the exception
     */
    Set<Map.Entry<String, Object>> getAttributes(String sessionId) throws Exception {
        GetSessionStateEntryProcessor entryProcessor = new GetSessionStateEntryProcessor();
        SessionState sessionState = (SessionState) executeOnKey(sessionId, entryProcessor);
        if (sessionState == null) {
            return null;
        }
        Map<String, Data> dataAttributes = sessionState.getAttributes();
        Set<Map.Entry<String, Object>> attributes = new HashSet<Map.Entry<String, Object>>(dataAttributes.size());
        for (Map.Entry<String, Data> entry : dataAttributes.entrySet()) {
            String key = entry.getKey();
            Object value = sss.getSerializationService().toObject(entry.getValue());
            attributes.add(new MapEntrySimple<String, Object>(key, value));
        }
        return attributes;
    }

    /**
     * Gets attribute.
     *
     * @param sessionId     the session id
     * @param attributeName the attribute name
     * @return the attribute
     * @throws Exception the exception
     */
    Object getAttribute(String sessionId, String attributeName) throws Exception {
        GetAttributeEntryProcessor entryProcessor = new GetAttributeEntryProcessor(attributeName);
        return executeOnKey(sessionId, entryProcessor);
    }

    /**
     * Delete attribute.
     *
     * @param sessionId     the session id
     * @param attributeName the attribute name
     * @throws Exception the exception
     */
    void deleteAttribute(String sessionId, String attributeName) throws Exception {
        setAttribute(sessionId, attributeName, null);
    }

    /**
     * Sets attribute.
     *
     * @param sessionId     the session id
     * @param attributeName the attribute name
     * @param value         the value
     * @throws Exception the exception
     */
    void setAttribute(String sessionId, String attributeName, Object value) throws Exception {
        Data dataValue = (value == null) ? null : sss.getSerializationService().toData(value);
        SessionUpdateEntryProcessor sessionUpdateProcessor = new SessionUpdateEntryProcessor(attributeName, dataValue);
        executeOnKey(sessionId, sessionUpdateProcessor);
    }

    /**
     * Delete session.
     *
     * @param sessionId  sessionId
     * @param invalidate if true remove the distributed session, otherwise just
     *                   remove the jvm reference
     * @return the boolean
     */
    public boolean deleteSession(String sessionId, boolean invalidate) {
        try {
            doDeleteSession(sessionId, invalidate);
            return true;
        } catch (Exception e) {
            orphanSessions.add(new AbstractMap.SimpleEntry(sessionId, invalidate));
            return false;
        }
    }

    private void doDeleteSession(String sessionId, boolean invalidate) throws Exception {
        DeleteSessionEntryProcessor entryProcessor = new DeleteSessionEntryProcessor(invalidate);
        executeOnKey(sessionId, entryProcessor);
    }

    /**
     * Gets attribute names.
     *
     * @param id the id
     * @return the attribute names
     * @throws Exception the exception
     */
    public Set<String> getAttributeNames(String id) throws Exception {
        return (Set<String>) executeOnKey(id, new GetAttributeNamesEntryProcessor());
    }

    /**
     * Update attributes.
     *
     * @param id      the id
     * @param updates the updates
     * @throws Exception the exception
     */
    public void updateAttributes(String id, Map<String, Object> updates) throws Exception {
        SerializationService ss = sss.getSerializationService();
        SessionUpdateEntryProcessor sessionUpdate = new SessionUpdateEntryProcessor(updates.size());
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            sessionUpdate.getAttributes().put(name, ss.toData(value));
        }
        executeOnKey(id, sessionUpdate);
    }

    /**
     * Destroy void.
     */
    public void destroy() {
        if (hazelcastInstance != null) {
            try {
                hazelcastInstance.getLifecycleService().shutdown();
                es.shutdown();
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
    }

    /**
     * Internal ThreadFactory to create threads which checks hazelcast instance
     */
    private static final class EnsureInstanceThreadFactory implements ThreadFactory {

        public Thread newThread(final Runnable r) {
            final Thread thread = new EnsureInstanceThread(r, ".hazelcast-wm.ensureInstance");
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * Runnable thread adapter to capture exceptions and notify Hazelcast about them
     */
    private static final class EnsureInstanceThread extends Thread {

        private EnsureInstanceThread(final Runnable target, final String name) {
            super(target, name);
        }

        public void run() {
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            }
        }
    }
}


