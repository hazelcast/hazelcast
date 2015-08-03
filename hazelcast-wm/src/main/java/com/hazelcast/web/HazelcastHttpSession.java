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

import com.hazelcast.util.EmptyStatement;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class HazelcastHttpSession implements HttpSession {

    private WebFilter webFilter;
    private volatile boolean valid = true;
    private final String id;
    private final HttpSession originalSession;
    private final Map<String, LocalCacheEntry> localCache = new ConcurrentHashMap<String, LocalCacheEntry>();

    private final boolean stickySession;
    private final boolean deferredWrite;
    // only true if session is created first time in the cluster
    private volatile boolean clusterWideNew;
    private Set<String> transientAttributes;

    public HazelcastHttpSession(WebFilter webFilter, final String sessionId, final HttpSession originalSession,
                                final boolean deferredWrite, final boolean stickySession) {
        this.webFilter = webFilter;
        this.id = sessionId;
        this.originalSession = originalSession;
        this.deferredWrite = deferredWrite;
        this.stickySession = stickySession;
        String transientAttributesParam = webFilter.getParam("transient-attributes");
        if (transientAttributesParam == null) {
            this.transientAttributes = Collections.emptySet();
        } else {
            this.transientAttributes = new HashSet<String>();
            StringTokenizer st = new StringTokenizer(transientAttributesParam, ",");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                this.transientAttributes.add(token.trim());
            }
        }
        buildLocalCache();
    }

    public HttpSession getOriginalSession() {
        return originalSession;
    }

    public String getOriginalSessionId() {
        return originalSession != null ? originalSession.getId() : null;
    }

    public void setAttribute(final String name, final Object value) {
        if (name == null) {
            throw new NullPointerException("name must not be null");
        }
        if (value == null) {
            removeAttribute(name);
            return;
        }
        boolean transientEntry = false;
        if (transientAttributes.contains(name)) {
            transientEntry = true;
        }
        LocalCacheEntry entry = localCache.get(name);
        if (entry == null || entry == WebFilter.NULL_ENTRY) {
            entry = new LocalCacheEntry(transientEntry);
            localCache.put(name, entry);
        }
        entry.setValue(value);
        entry.setDirty(true);
        entry.setRemoved(false);
        entry.setReload(false);
        if (!deferredWrite && !transientEntry) {
            try {
                webFilter.getClusteredSessionService().setAttribute(id, name, value);
                entry.setDirty(false);
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
    }

    public Object getAttribute(final String name) {
        LocalCacheEntry cacheEntry = localCache.get(name);
        Object value = null;

        if (cacheEntry == null || cacheEntry.isReload()) {
            try {
                value = webFilter.getClusteredSessionService().getAttribute(id, name);
                if (value == null) {
                    return null;
                }
                cacheEntry = new LocalCacheEntry(false, value);
                cacheEntry.setReload(false);
                localCache.put(name, cacheEntry);
            } catch (Exception e) {
                WebFilter.LOGGER.warning("session could not be load so you might be dealing with stale data", e);
                if (cacheEntry == null) {
                    return null;
                }
            }
        }
        if (cacheEntry.isRemoved()) {
            return null;
        }
        return cacheEntry.getValue();
    }

    public Enumeration<String> getAttributeNames() {
        final Set<String> keys = selectKeys();
        return new Enumeration<String>() {
            private final String[] elements = keys.toArray(new String[keys.size()]);
            private int index;

            @Override
            public boolean hasMoreElements() {
                return index < elements.length;
            }

            @Override
            public String nextElement() {
                return elements[index++];
            }
        };
    }

    public String getId() {
        return id;
    }

    public ServletContext getServletContext() {
        return webFilter.servletContext;
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public HttpSessionContext getSessionContext() {
        return originalSession.getSessionContext();
    }

    public Object getValue(final String name) {
        return getAttribute(name);
    }

    public String[] getValueNames() {
        final Set<String> keys = selectKeys();
        return keys.toArray(new String[keys.size()]);
    }

    public void invalidate() {
        // we must invalidate hazelcast session first
        // invalidating original session will trigger another
        // invalidation as our SessionListener will be triggered.
        webFilter.destroySession(this, true);
        originalSession.invalidate();
    }

    public boolean isNew() {
        return originalSession.isNew() && clusterWideNew;
    }

    public void putValue(final String name, final Object value) {
        setAttribute(name, value);
    }

    public void removeAttribute(final String name) {
        LocalCacheEntry entry = localCache.get(name);
        if (entry != null && entry != WebFilter.NULL_ENTRY) {
            entry.setValue(null);
            entry.setRemoved(true);
            entry.setDirty(true);
            entry.setReload(false);
        }
        if (!deferredWrite) {
            try {
                webFilter.getClusteredSessionService().deleteAttribute(id, name);
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
    }

    public void removeValue(final String name) {
        removeAttribute(name);
    }

    /**
     * @return {@code true} if {@link #deferredWrite} is enabled <i>and</i> at least one entry in the local
     * cache is dirty; otherwise, {@code false}
     */
    public boolean sessionChanged() {
        for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
            if (entry.getValue().isDirty()) {
                return true;
            }
        }
        return false;
    }

    public long getCreationTime() {
        return originalSession.getCreationTime();
    }

    public long getLastAccessedTime() {
        return originalSession.getLastAccessedTime();
    }

    public int getMaxInactiveInterval() {
        return originalSession.getMaxInactiveInterval();
    }

    public void setMaxInactiveInterval(int maxInactiveSeconds) {
        originalSession.setMaxInactiveInterval(maxInactiveSeconds);
    }

    void destroy(boolean invalidate) {
        valid = false;
        webFilter.getClusteredSessionService().deleteSession(id, invalidate);
    }

    public boolean isValid() {
        return valid;
    }

    private void buildLocalCache() {
        Set<Map.Entry<String, Object>> entrySet = null;
        try {
            entrySet = webFilter.getClusteredSessionService().getAttributes(id);
        } catch (Exception e) {
            return;
        }
        if (entrySet != null) {
            for (Map.Entry<String, Object> entry : entrySet) {
                String attributeKey = entry.getKey();
                LocalCacheEntry cacheEntry = localCache.get(attributeKey);
                if (cacheEntry == null) {
                    cacheEntry = new LocalCacheEntry(transientAttributes.contains(attributeKey));

                    localCache.put(attributeKey, cacheEntry);
                }
                if (WebFilter.LOGGER.isFinestEnabled()) {
                    WebFilter.LOGGER.finest("Storing " + attributeKey + " on session " + id);
                }
                cacheEntry.setValue(entry.getValue());
                cacheEntry.setDirty(false);
            }
        }
    }

    void sessionDeferredWrite() {
        if (sessionChanged() || isNew()) {
            Map<String, Object> updates = new HashMap<String, Object>();

            Iterator<Map.Entry<String, LocalCacheEntry>> iterator = localCache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, LocalCacheEntry> entry = iterator.next();
                LocalCacheEntry cacheEntry = entry.getValue();

                if (cacheEntry.isDirty() && !cacheEntry.isTransient()) {
                    if (cacheEntry.isRemoved()) {
                        updates.put(entry.getKey(), null);
                    } else {
                        updates.put(entry.getKey(), cacheEntry.getValue());
                    }
                    cacheEntry.setDirty(false);

                }
            }

            try {
                webFilter.getClusteredSessionService().updateAttributes(id, updates);
            } catch (Exception ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
    }

    private Set<String> selectKeys() {
        Set<String> keys = new HashSet<String>();
        if (!deferredWrite) {
            Set<String> attributeNames = null;
            try {
                attributeNames = webFilter.getClusteredSessionService().getAttributeNames(id);
            } catch (Exception ignored) {
                for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                    if (!entry.getValue().isRemoved() && entry.getValue() != WebFilter.NULL_ENTRY) {
                        keys.add(entry.getKey());
                    }
                }
            }
            if (attributeNames != null) {
                keys.addAll(attributeNames);
            }
        } else {
            for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                if (!entry.getValue().isRemoved() && entry.getValue() != WebFilter.NULL_ENTRY) {
                    keys.add(entry.getKey());
                }
            }
        }
        return keys;
    }

    public void setClusterWideNew(boolean clusterWideNew) {
        this.clusterWideNew = clusterWideNew;
    }

    public boolean isStickySession() {
        return stickySession;
    }

    public void updateReloadFlag() {
        for (Map.Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
            entry.getValue().setReload(true);
        }

    }
} // END of HazelSession
