/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.UuidUtil;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.web.HazelcastInstanceLoader.*;

@SuppressWarnings("deprecation")
public class WebFilter implements Filter {

    private static final ILogger logger = Logger.getLogger(WebFilter.class);

    private static final LocalCacheEntry NULL_ENTRY = new LocalCacheEntry();

    private static final String HAZELCAST_REQUEST = "*hazelcast-request";

    private static final String HAZELCAST_SESSION_COOKIE_NAME = "hazelcast.sessionId";

    static final String HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR = "::hz::";

    private static final ConcurrentMap<String, String> mapOriginalSessions = new ConcurrentHashMap<String, String>(1000);

    private static final ConcurrentMap<String, HazelcastHttpSession> mapSessions = new ConcurrentHashMap<String, HazelcastHttpSession>(1000);

    private HazelcastInstance hazelcastInstance;

    private String clusterMapName = "none";

    private String sessionCookieName = HAZELCAST_SESSION_COOKIE_NAME;

    private String sessionCookieDomain = null;

    private boolean sessionCookieSecure = false;

    private boolean sessionCookieHttpOnly = false;

    private boolean stickySession = true;

    private boolean shutdownOnDestroy = true;

    private boolean deferredWrite = false;

    private Properties properties;

    protected ServletContext servletContext;

    protected FilterConfig filterConfig;

    public WebFilter() {
    }

    public WebFilter(Properties properties) {
        this();
        this.properties = properties;
    }

    public final void init(final FilterConfig config) throws ServletException {
        filterConfig = config;
        servletContext = config.getServletContext();
        initInstance();
        String mapName = getParam("map-name");
        if (mapName != null) {
            clusterMapName = mapName;
        } else {
            clusterMapName = "_web_" + servletContext.getServletContextName();
        }
        try {
            Config hzConfig = hazelcastInstance.getConfig();
            String sessionTTL = getParam("session-ttl-seconds");
            if (sessionTTL != null) {
                MapConfig mapConfig = hzConfig.getMapConfig(clusterMapName);
                mapConfig.setTimeToLiveSeconds(Integer.valueOf(sessionTTL));
                hzConfig.addMapConfig(mapConfig);
            }
        } catch (UnsupportedOperationException ignored) {
            // client cannot access Config.
        }
        String cookieName = getParam("cookie-name");
        if (cookieName != null) {
            sessionCookieName = cookieName;
        }
        String cookieDomain = getParam("cookie-domain");
        if (cookieDomain != null) {
            sessionCookieDomain = cookieDomain;
        }
        String cookieSecure = getParam("cookie-secure");
        if (cookieSecure != null) {
            sessionCookieSecure = Boolean.valueOf(cookieSecure);
        }
        String cookieHttpOnly = getParam("cookie-http-only");
        if (cookieHttpOnly != null) {
            sessionCookieHttpOnly = Boolean.valueOf(cookieHttpOnly);
        }
        String stickySessionParam = getParam("sticky-session");
        if (stickySessionParam != null) {
            stickySession = Boolean.valueOf(stickySessionParam);
        }
        String shutdownOnDestroyParam = getParam("shutdown-on-destroy");
        if (shutdownOnDestroyParam != null) {
            shutdownOnDestroy = Boolean.valueOf(shutdownOnDestroyParam);
        }
        String deferredWriteParam = getParam("deferred-write");
        if (deferredWriteParam != null) {
            deferredWrite = Boolean.parseBoolean(deferredWriteParam);
        }
        if (!stickySession) {
            getClusterMap().addEntryListener(new EntryListener<String, Object>() {
                public void entryAdded(EntryEvent<String, Object> entryEvent) {
                }

                public void entryRemoved(EntryEvent<String, Object> entryEvent) {
                    if (entryEvent.getMember() == null || // client events has no owner member
                            !entryEvent.getMember().localMember()) {
                        removeSessionLocally(entryEvent.getKey());
                    }
                }

                public void entryUpdated(EntryEvent<String, Object> entryEvent) {
                }

                public void entryEvicted(EntryEvent<String, Object> entryEvent) {
                    entryRemoved(entryEvent);
                }
            }, false);
        }

        if(logger.isLoggable(Level.FINEST)){
            logger.finest("sticky:" + stickySession + ", shutdown-on-destroy: " + shutdownOnDestroy
                    + ", map-name: " + clusterMapName);
        }
    }

    private void initInstance() throws ServletException {
        if (properties == null) {
            properties = new Properties();
        }
        setProperty(CONFIG_LOCATION);
        setProperty(INSTANCE_NAME);
        setProperty(USE_CLIENT);
        setProperty(CLIENT_CONFIG_LOCATION);
        hazelcastInstance = getInstance(properties);
    }

    private void setProperty(String propertyName) {
        String value = getParam(propertyName);
        if (value != null) {
            properties.setProperty(propertyName, value);
        }
    }

    private void removeSessionLocally(String sessionId) {
        HazelcastHttpSession hazelSession = mapSessions.remove(sessionId);
        if (hazelSession != null) {
            mapOriginalSessions.remove(hazelSession.originalSession.getId());
            if(logger.isLoggable(Level.FINEST)){
                logger.finest("Destroying session locally " + hazelSession);
            }
            hazelSession.destroy();
        }
    }

    static void destroyOriginalSession(HttpSession originalSession) {
        String hazelcastSessionId = mapOriginalSessions.remove(originalSession.getId());
        if (hazelcastSessionId != null) {
            HazelcastHttpSession hazelSession = mapSessions.remove(hazelcastSessionId);
            if (hazelSession != null) {
                hazelSession.webFilter.destroySession(hazelSession, false);
            }
        }
    }

       private String extractAttributeKey(String key) {
        return key.substring(key.indexOf(HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR) + HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR.length());
    }

    private HazelcastHttpSession createNewSession(RequestWrapper requestWrapper, String existingSessionId) {
        String id = existingSessionId != null ? existingSessionId : generateSessionId();
        if (requestWrapper.getOriginalSession(false) != null) {
            logger.finest("Original session exists!!!");
        }
        HttpSession originalSession = requestWrapper.getOriginalSession(true);
        HazelcastHttpSession hazelcastSession = new HazelcastHttpSession(WebFilter.this, id, originalSession, deferredWrite);
        mapSessions.put(hazelcastSession.getId(), hazelcastSession);
        String oldHazelcastSessionId = mapOriginalSessions.put(originalSession.getId(), hazelcastSession.getId());
        if (oldHazelcastSessionId != null) {
            if(logger.isFinestEnabled()){
                logger.finest("!!! Overriding an existing hazelcastSessionId " + oldHazelcastSessionId);
            }
        }
        if(logger.isFinestEnabled()){
            logger.finest("Created new session with id: " + id);
            logger.finest(mapSessions.size() + " is sessions.size and originalSessions.size: " + mapOriginalSessions.size());
        }
        addSessionCookie(requestWrapper, id);
        if (deferredWrite) {
            loadHazelcastSession(hazelcastSession);
        }
        return hazelcastSession;
    }

    private void loadHazelcastSession(HazelcastHttpSession hazelcastSession) {
        Set<Entry<String, Object>> entrySet = getClusterMap().entrySet(new SessionAttributePredicate(hazelcastSession.getId()));
        Map<String, LocalCacheEntry> cache = hazelcastSession.localCache;
        for (Entry<String, Object> entry : entrySet) {
            String attributeKey = extractAttributeKey(entry.getKey());
            LocalCacheEntry cacheEntry = cache.get(attributeKey);
            if (cacheEntry == null) {
                cacheEntry = new LocalCacheEntry();
                cache.put(attributeKey, cacheEntry);
            }
            if(logger.isFinestEnabled()){
                logger.finest("Storing " + attributeKey + " on session " + hazelcastSession.getId());
            }
            cacheEntry.value = entry.getValue();
            cacheEntry.dirty = false;
        }
    }

    private void prepareReloadingSession(HazelcastHttpSession hazelcastSession) {
        if (deferredWrite && hazelcastSession != null) {
            Map<String, LocalCacheEntry> cache = hazelcastSession.localCache;
            for (LocalCacheEntry cacheEntry : cache.values()) {
                cacheEntry.reload = true;
            }
        }
    }

    /**
     * Destroys a session, determining if it should be destroyed clusterwide automatically or via expiry.
     *
     * @param session             The session to be destroyed
     * @param removeGlobalSession boolean value - true if the session should be destroyed irrespective of active time
     */
    private void destroySession(HazelcastHttpSession session, boolean removeGlobalSession) {
        if(logger.isFinestEnabled()){
            logger.finest("Destroying local session: " + session.getId());
        }
        mapSessions.remove(session.getId());
        mapOriginalSessions.remove(session.originalSession.getId());
        session.destroy();
        if (removeGlobalSession) {
            if(logger.isFinestEnabled()){
                logger.finest("Destroying cluster session: " + session.getId() + " => Ignore-timeout: true");
            }
            IMap<String, Object> clusterMap = getClusterMap();
            clusterMap.delete(session.getId());
            clusterMap.executeOnEntries(new InvalidateEntryProcessor(session.getId()));
        }
    }

    private IMap<String, Object> getClusterMap() {
        return hazelcastInstance.getMap(clusterMapName);
    }

    private HazelcastHttpSession getSessionWithId(final String sessionId) {
        HazelcastHttpSession session = mapSessions.get(sessionId);
        if (session != null && !session.isValid()) {
            destroySession(session, true);
            session = null;
        }
        return session;
    }

    private class RequestWrapper extends HttpServletRequestWrapper {
        HazelcastHttpSession hazelcastSession = null;

        final ResponseWrapper res;

        String requestedSessionId;

        public RequestWrapper(final HttpServletRequest req,
                              final ResponseWrapper res) {
            super(req);
            this.res = res;
            req.setAttribute(HAZELCAST_REQUEST, this);
        }

        public void setHazelcastSession(HazelcastHttpSession hazelcastSession, String requestedSessionId) {
            this.hazelcastSession = hazelcastSession;
            this.requestedSessionId = requestedSessionId;
        }

        HttpSession getOriginalSession(boolean create) {
            return super.getSession(create);
        }

        @Override
        public RequestDispatcher getRequestDispatcher(final String path) {
            final ServletRequest original = getRequest();
            return new RequestDispatcher() {
                public void forward(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
                    original.getRequestDispatcher(path).forward(RequestWrapper.this, servletResponse);
                }

                public void include(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
                    original.getRequestDispatcher(path).include(RequestWrapper.this, servletResponse);
                }
            };
        }

        public String fetchHazelcastSessionId() {
            if (requestedSessionId != null) {
                return requestedSessionId;
            }
            requestedSessionId = getSessionCookie(this);
            if (requestedSessionId != null) {
                return requestedSessionId;
            }
            requestedSessionId = getParameter(HAZELCAST_SESSION_COOKIE_NAME);
            return requestedSessionId;
        }

        @Override
        public HttpSession getSession() {
            return getSession(true);
        }

        @Override
        public HazelcastHttpSession getSession(final boolean create) {
            if (hazelcastSession != null && !hazelcastSession.isValid()) {
                logger.finest("Session is invalid!");
                destroySession(hazelcastSession, true);
                hazelcastSession = null;
            }
            if (hazelcastSession == null) {
                HttpSession originalSession = getOriginalSession(false);
                if (originalSession != null) {
                    String hazelcastSessionId = mapOriginalSessions.get(originalSession.getId());
                    if (hazelcastSessionId != null) {
                        hazelcastSession = mapSessions.get(hazelcastSessionId);
                    }
                    if (hazelcastSession == null) {
                        mapOriginalSessions.remove(originalSession.getId());
                        originalSession.invalidate();
                    }
                }
            }
            if (hazelcastSession != null)
                return hazelcastSession;
            final String requestedSessionId = fetchHazelcastSessionId();
            if (requestedSessionId != null) {
                hazelcastSession = getSessionWithId(requestedSessionId);
                if (hazelcastSession == null) {
                    final Boolean existing = (Boolean) getClusterMap().get(requestedSessionId);
                    if (existing != null && existing) {
                        // we already have the session in the cluster loading it...
                        hazelcastSession = createNewSession(RequestWrapper.this, requestedSessionId);
                    }
                }
            }
            if (hazelcastSession == null && create) {
                hazelcastSession = createNewSession(RequestWrapper.this, null);
            }
            if (deferredWrite) {
                prepareReloadingSession(hazelcastSession);
            }
            return hazelcastSession;
        }
    } // END of RequestWrapper

    private class ResponseWrapper extends HttpServletResponseWrapper {

        public ResponseWrapper(final HttpServletResponse original) {
            super(original);
        }
    }

    private static class LocalCacheEntry {
        private Object value;
        volatile boolean dirty = false;
        volatile boolean reload = false;
        boolean removed = false; // does not need to be volatile - it's piggybacked on dirty!
    }

    private class HazelcastHttpSession implements HttpSession {
        private final Map<String, LocalCacheEntry> localCache;

        private final boolean deferredWrite;

        volatile boolean valid = true;

        final String id;

        final HttpSession originalSession;

        final WebFilter webFilter;

        public HazelcastHttpSession(WebFilter webFilter, final String sessionId, HttpSession originalSession, boolean deferredWrite) {
            this.webFilter = webFilter;
            this.id = sessionId;
            this.originalSession = originalSession;
            this.deferredWrite = deferredWrite;
            this.localCache = deferredWrite ? new ConcurrentHashMap<String, LocalCacheEntry>() : null;
        }

        public Object getAttribute(final String name) {
            IMap<String, Object> clusterMap = getClusterMap();
            if (deferredWrite) {
                LocalCacheEntry cacheEntry = localCache.get(name);
                if (cacheEntry == null || cacheEntry.reload) {
                    Object value = clusterMap.get(buildAttributeName(name));
                    if (value == null) {
                        cacheEntry = NULL_ENTRY;
                    } else {
                        cacheEntry = new LocalCacheEntry();
                        cacheEntry.value = value;
                        cacheEntry.reload = false;
                    }
                    localCache.put(name, NULL_ENTRY);
                }
                return cacheEntry != NULL_ENTRY ? cacheEntry.value : null;
            }
            return clusterMap.get(buildAttributeName(name));
        }

        public Enumeration<String> getAttributeNames() {
            final Set<String> keys = selectKeys();
            return new Enumeration<String>() {
                private final String[] elements = keys.toArray(new String[keys.size()]);
                private int index = 0;

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
            return servletContext;
        }

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
            originalSession.invalidate();
            destroySession(this, true);
        }

        public boolean isNew() {
            return originalSession.isNew();
        }

        public void putValue(final String name, final Object value) {
            setAttribute(name, value);
        }

        public void removeAttribute(final String name) {
            if (deferredWrite) {
                LocalCacheEntry entry = localCache.get(name);
                if (entry != null) {
                    entry.value = null;
                    entry.removed = true;
                    // dirty needs to be set as last value for memory visibility reasons!
                    entry.dirty = true;
                }
            } else {
                getClusterMap().delete(buildAttributeName(name));
            }
        }

        public void setAttribute(final String name, final Object value) {
            if (name == null) {
                throw new NullPointerException("name must not be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("value must not be null");
            }
            if (deferredWrite) {
                LocalCacheEntry entry = localCache.get(name);
                if (entry == null) {
                    entry = new LocalCacheEntry();
                    localCache.put(name, entry);
                }
                entry.value = value;
                entry.dirty = true;
            } else {
                getClusterMap().put(buildAttributeName(name), value);
            }
        }

        public void removeValue(final String name) {
            removeAttribute(name);
        }

        public boolean sessionChanged() {
            if (!deferredWrite) {
                return false;
            }
            for (Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                if (entry.getValue().dirty) {
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

        void destroy() {
            valid = false;
        }

        public boolean isValid() {
            return valid;
        }

        private String buildAttributeName(String name) {
            return id + HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR + name;
        }

        private void sessionDeferredWrite() {
            IMap<String, Object> clusterMap = getClusterMap();
            if (deferredWrite) {
                Iterator<Entry<String, LocalCacheEntry>> iterator = localCache.entrySet().iterator();
                while(iterator.hasNext()) {
                    Entry<String, LocalCacheEntry> entry = iterator.next();
                    if (entry.getValue().dirty) {
                        LocalCacheEntry cacheEntry = entry.getValue();
                        if (cacheEntry.removed) {
                            clusterMap.delete(buildAttributeName(entry.getKey()));
                            iterator.remove();
                        } else {
                            clusterMap.put(buildAttributeName(entry.getKey()), cacheEntry.value);
                            cacheEntry.dirty = false;
                        }
                    }
                }
            }
            if (!clusterMap.containsKey(id)) {
                clusterMap.put(id, Boolean.TRUE);
            }
        }

        private Set<String> selectKeys() {
            if (!deferredWrite) {
                return getClusterMap().keySet(new SessionAttributePredicate(id));
            }
            Set<String> keys = new HashSet<String>();
            Iterator<Entry<String, LocalCacheEntry>> iterator = localCache.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<String, LocalCacheEntry> entry = iterator.next();
                if (!entry.getValue().removed) {
                    keys.add(entry.getKey());
                }
            }
            return keys;
        }
    }// END of HazelSession

    private static synchronized String generateSessionId() {
        final String id = UuidUtil.buildRandomUuidString();
        final StringBuilder sb = new StringBuilder("HZ");
        final char[] chars = id.toCharArray();
        for (final char c : chars) {
            if (c != '-') {
                if (Character.isLetter(c)) {
                    sb.append(Character.toUpperCase(c));
                } else
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    private void addSessionCookie(final RequestWrapper req, final String sessionId) {
        final Cookie sessionCookie = new Cookie(sessionCookieName, sessionId);
        String path = req.getContextPath();
        if ("".equals(path)) {
            path = "/";
        }
        sessionCookie.setPath(path);
        sessionCookie.setMaxAge(-1);
        if (sessionCookieDomain != null) {
            sessionCookie.setDomain(sessionCookieDomain);
        }
        try {
            sessionCookie.setHttpOnly(sessionCookieHttpOnly);
        } catch (NoSuchMethodError e) {
            // must be servlet spec before 3.0, don't worry about it!
        }
        sessionCookie.setSecure(sessionCookieSecure);
        req.res.addCookie(sessionCookie);
    }

    private String getSessionCookie(final RequestWrapper req) {
        final Cookie[] cookies = req.getCookies();
        if (cookies != null) {
            for (final Cookie cookie : cookies) {
                final String name = cookie.getName();
                final String value = cookie.getValue();
                if (name.equalsIgnoreCase(sessionCookieName)) {
                    return value;
                }
            }
        }
        return null;
    }

    public final void doFilter(ServletRequest req, ServletResponse res, final FilterChain chain)
            throws IOException, ServletException {
        if (!(req instanceof HttpServletRequest)) {
            chain.doFilter(req, res);
        } else {
            if (req instanceof RequestWrapper) {
                logger.finest("Request is instance of RequestWrapper! Continue...");
                chain.doFilter(req, res);
                return;
            }
            HttpServletRequest httpReq = (HttpServletRequest) req;
            RequestWrapper existingReq = (RequestWrapper) req.getAttribute(HAZELCAST_REQUEST);
            final ResponseWrapper resWrapper = new ResponseWrapper((HttpServletResponse) res);
            final RequestWrapper reqWrapper = new RequestWrapper(httpReq, resWrapper);
            if (existingReq != null) {
                reqWrapper.setHazelcastSession(existingReq.hazelcastSession, existingReq.requestedSessionId);
            }
            chain.doFilter(reqWrapper, resWrapper);
            if (existingReq != null) return;
            HazelcastHttpSession session = reqWrapper.getSession(false);
            if (session != null && session.isValid()) {
                if (session.sessionChanged() || !deferredWrite) {
                    if(logger.isFinestEnabled()){
                        logger.finest("PUTTING SESSION " + session.getId());
                    }
                    session.sessionDeferredWrite();
                }
            }
        }
    }

    public final void destroy() {
        mapSessions.clear();
        mapOriginalSessions.clear();
        shutdownInstance();
    }

    protected HazelcastInstance getInstance(Properties properties) throws ServletException {
        return HazelcastInstanceLoader.createInstance(filterConfig, properties);
    }

    protected void shutdownInstance() {
        if (shutdownOnDestroy && hazelcastInstance != null) {
            hazelcastInstance.getLifecycleService().shutdown();
        }
    }

    private String getParam(String name) {
        if (properties != null && properties.containsKey(name)) {
            return properties.getProperty(name);
        } else {
            return filterConfig.getInitParameter(name);
        }
    }
}// END of WebFilter

