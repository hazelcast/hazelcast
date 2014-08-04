/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.MapEvent;
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

/**
 * Provides clustered sessions by backing session data with an {@link IMap}.
 * <p/>
 * Using this filter requires also registering a {@link SessionListener} to provide session timeout notifications.
 * Failure to register the listener when using this filter will result in session state getting out of sync between
 * the servlet container and Hazelcast.
 * <p/>
 * This filter supports the following {@code &lt;init-param&gt;} values:
 * <ul>
 *     <li>{@code use-client}: When enabled, a {@link com.hazelcast.client.HazelcastClient HazelcastClient} is
 *     used to connect to the cluster, rather than joining as a full node. (Default: {@code false})</li>
 *     <li>{@code config-location}: Specifies the location of an XML configuration file that can be used to
 *     initialize the {@link HazelcastInstance} (Default: None; the {@link HazelcastInstance} is initialized
 *     using its own defaults)</li>
 *     <li>{@code client-config-location}: Specifies the location of an XML configuration file that can be
 *     used to initialize the {@link HazelcastInstance}. <i>This setting is only checked when {@code use-client}
 *     is set to {@code true}.</i> (Default: Falls back on {@code config-location})</li>
 *     <li>{@code instance-name}: Names the {@link HazelcastInstance}. This can be used to reference an already-
 *     initialized {@link HazelcastInstance} in the same JVM (Default: The configured instance name, or a
 *     generated name if the configuration does not specify a value)</li>
 *     <li>{@code shutdown-on-destroy}: When enabled, shuts down the {@link HazelcastInstance} when the filter is
 *     destroyed (Default: {@code true})</li>
 *     <li>{@code map-name}: Names the {@link IMap} the filter should use to persist session details (Default:
 *     {@code "_web_" + ServletContext.getServletContextName()}; e.g. "_web_MyApp")</li>
 *     <li>{@code session-ttl-seconds}: Sets the {@link MapConfig#setTimeToLiveSeconds(int) time-to-live} for
 *     the {@link IMap} used to persist session details (Default: Uses the existing {@link MapConfig} setting
 *     for the {@link IMap}, which defaults to infinite)</li>
 *     <li>{@code sticky-session}: When enabled, optimizes {@link IMap} interactions by assuming individual sessions
 *     are only used from a single node (Default: {@code true})</li>
 *     <li>{@code deferred-write}: When enabled, optimizes {@link IMap} interactions by only writing session attributes
 *     at the end of a request. This can yield significant performance improvements for session-heavy applications
 *     (Default: {@code false})</li>
 *     <li>{@code cookie-name}: Sets the name for the Hazelcast session cookie (Default:
 *     {@link #HAZELCAST_SESSION_COOKIE_NAME "hazelcast.sessionId"}</li>
 *     <li>{@code cookie-domain}: Sets the domain for the Hazelcast session cookie (Default: {@code null})</li>
 *     <li>{@code cookie-secure}: When enabled, indicates the Hazelcast session cookie should only be sent over
 *     secure protocols (Default: {@code false})</li>
 *     <li>{@code cookie-http-only}: When enabled, marks the Hazelcast session cookie as "HttpOnly", indicating
 *     it should not be available to scripts (Default: {@code false})
 *     <ul>
 *         <li>{@code cookie-http-only} requires a Servlet 3.0-compatible container, such as Tomcat 7+ or Jetty 8+</li>
 *     </ul>
 *     </li>
 * </ul>
 */
public class WebFilter implements Filter {

    protected static final String HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR = "::hz::";

    private static final ILogger LOGGER = Logger.getLogger(WebFilter.class);
    private static final LocalCacheEntry NULL_ENTRY = new LocalCacheEntry();
    private static final String HAZELCAST_REQUEST = "*hazelcast-request";
    private static final String HAZELCAST_SESSION_COOKIE_NAME = "hazelcast.sessionId";

    protected ServletContext servletContext;
    protected FilterConfig filterConfig;

    private final ConcurrentMap<String, String> originalSessions = new ConcurrentHashMap<String, String>(1000);
    private final ConcurrentMap<String, HazelcastHttpSession> sessions =
            new ConcurrentHashMap<String, HazelcastHttpSession>(1000);

    private String sessionCookieName = HAZELCAST_SESSION_COOKIE_NAME;
    private HazelcastInstance hazelcastInstance;
    private String clusterMapName = "none";
    private String sessionCookieDomain;
    private boolean sessionCookieSecure;
    private boolean sessionCookieHttpOnly;
    private boolean stickySession = true;
    private boolean shutdownOnDestroy = true;
    private boolean deferredWrite;
    private Properties properties;

    public WebFilter() {
    }

    public WebFilter(Properties properties) {
        this.properties = properties;
    }

    void destroyOriginalSession(HttpSession originalSession) {
        String hazelcastSessionId = originalSessions.remove(originalSession.getId());
        if (hazelcastSessionId != null) {
            HazelcastHttpSession hazelSession = sessions.remove(hazelcastSessionId);
            if (hazelSession != null) {
                destroySession(hazelSession, false);
            }
        }
    }

    private static String generateSessionId() {
        final String id = UuidUtil.buildRandomUuidString();
        final StringBuilder sb = new StringBuilder("HZ");
        final char[] chars = id.toCharArray();
        for (final char c : chars) {
            if (c != '-') {
                if (Character.isLetter(c)) {
                    sb.append(Character.toUpperCase(c));
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    public final void init(final FilterConfig config)
            throws ServletException {
        filterConfig = config;

        // Register the WebFilter with the ServletContext so SessionListener can look it up. The name
        // here is WebFilter.class instead of getClass() because WebFilter can have subclasses
        servletContext = config.getServletContext();
        servletContext.setAttribute(WebFilter.class.getName(), this);

        initInstance();
        initCookieParams();
        initParams();

        String mapName = getParam("map-name");
        if (mapName != null) {
            clusterMapName = mapName;
        } else {
            clusterMapName = "_web_" + servletContext.getServletContextName();
        }

        try {
            String sessionTTL = getParam("session-ttl-seconds");
            if (sessionTTL != null) {
                Config hzConfig = hazelcastInstance.getConfig();

                MapConfig mapConfig = hzConfig.getMapConfig(clusterMapName);
                mapConfig.setTimeToLiveSeconds(Integer.parseInt(sessionTTL));

                hzConfig.addMapConfig(mapConfig);
            }
        } catch (UnsupportedOperationException ignored) {
            LOGGER.info("client cannot access Config.");
        }

        if (!stickySession) {
            getClusterMap().addEntryListener(new EntryListener<String, Object>() {
                public void entryAdded(EntryEvent<String, Object> entryEvent) {
                }

                public void entryRemoved(EntryEvent<String, Object> entryEvent) {
                    if (entryEvent.getMember() == null || !entryEvent.getMember().localMember()) {
                        removeSessionLocally(entryEvent.getKey());
                    }
                }

                public void entryUpdated(EntryEvent<String, Object> entryEvent) {
                }

                public void entryEvicted(EntryEvent<String, Object> entryEvent) {
                    entryRemoved(entryEvent);
                }

                @Override
                public void mapEvicted(MapEvent event) {
                    // this method should be updated if we internally call evictAll in session replication logic
                }

                @Override
                public void mapCleared(MapEvent event) {
                    // this method should be updated if we internally call clearAll in session replication logic
                }
            }, false);
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("sticky:" + stickySession + ", shutdown-on-destroy: " + shutdownOnDestroy
                    + ", map-name: " + clusterMapName);
        }
    }

    private void initParams() {
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
    }

    private void initCookieParams() {
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
    }

    private void initInstance() throws ServletException {
        if (properties == null) {
            properties = new Properties();
        }
        setProperty(HazelcastInstanceLoader.CONFIG_LOCATION);
        setProperty(HazelcastInstanceLoader.INSTANCE_NAME);
        setProperty(HazelcastInstanceLoader.USE_CLIENT);
        setProperty(HazelcastInstanceLoader.CLIENT_CONFIG_LOCATION);
        hazelcastInstance = getInstance(properties);
    }

    private void setProperty(String propertyName) {
        String value = getParam(propertyName);
        if (value != null) {
            properties.setProperty(propertyName, value);
        }
    }

    private void removeSessionLocally(String sessionId) {
        HazelcastHttpSession hazelSession = sessions.remove(sessionId);
        if (hazelSession != null) {
            originalSessions.remove(hazelSession.originalSession.getId());
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("Destroying session locally " + hazelSession);
            }
            hazelSession.destroy();
        }
    }

    private String extractAttributeKey(String key) {
        return key.substring(key.indexOf(HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR) + HAZELCAST_SESSION_ATTRIBUTE_SEPARATOR.length());
    }

    private HazelcastHttpSession createNewSession(RequestWrapper requestWrapper, String existingSessionId) {
        String id = existingSessionId == null ? generateSessionId() : existingSessionId;
        if (requestWrapper.getOriginalSession(false) != null) {
            LOGGER.finest("Original session exists!!!");
        }

        HttpSession originalSession = requestWrapper.getOriginalSession(true);
        HazelcastHttpSession hazelcastSession = new HazelcastHttpSession(id, originalSession, deferredWrite);
        if (existingSessionId == null) {
            hazelcastSession.setClusterWideNew(true);
            // If the session is being created for the first time, add its initial reference in the cluster-wide map.
            getClusterMap().executeOnKey(id, new AddSessionEntryProcessor());
        }

        updateSessionMaps(id, originalSession, hazelcastSession);
        addSessionCookie(requestWrapper, id);

        return hazelcastSession;
    }

    private void prepareReloadingSession(HazelcastHttpSession hazelcastSession) {
        if (deferredWrite && hazelcastSession != null) {
            Map<String, LocalCacheEntry> cache = hazelcastSession.localCache;
            for (LocalCacheEntry cacheEntry : cache.values()) {
                cacheEntry.reload = true;
            }
        }
    }

    private void updateSessionMaps(String sessionId, HttpSession originalSession, HazelcastHttpSession hazelcastSession) {
        sessions.put(hazelcastSession.getId(), hazelcastSession);
        String oldHazelcastSessionId = originalSessions.put(originalSession.getId(), hazelcastSession.getId());
        if (LOGGER.isFinestEnabled()) {
            if (oldHazelcastSessionId != null) {
                LOGGER.finest("!!! Overwrote an existing hazelcastSessionId " + oldHazelcastSessionId);
            }
            LOGGER.finest("Created new session with id: " + sessionId);
            LOGGER.finest(sessions.size() + " is sessions.size and originalSessions.size: " + originalSessions.size());
        }
    }

    /**
     * Destroys a session, determining if it should be destroyed clusterwide automatically or via expiry.
     *
     * @param session    the session to be destroyed <i>locally</i>
     * @param invalidate {@code true} if the session has been invalidated and should be destroyed on all nodes
     *                   in the cluster; otherwise, {@code false} to only remove the session globally if this
     *                   node was the final node referencing it
     */
    private void destroySession(HazelcastHttpSession session, boolean invalidate) {
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("Destroying local session: " + session.getId());
        }
        sessions.remove(session.getId());
        originalSessions.remove(session.originalSession.getId());
        session.destroy();

        final IMap<String, Object> clusterMap = getClusterMap();
        final boolean invalidated;
        if (invalidate) {
            if (LOGGER.isFinestEnabled()) {
                LOGGER.finest("Destroying cluster session: " + session.getId() + " => Ignore-timeout: true");
            }
            clusterMap.delete(session.getId());
            invalidated = true;
        } else {
            Boolean destroyed = (Boolean) clusterMap.executeOnKey(session.getId(), new DestroySessionEntryProcessor());

            invalidated = (destroyed != null && destroyed);
        }

        if (invalidated) {
            // If the session was invalidated, either explicitly or because the final reference to it was
            // destroyed, invalidate all of the attributes that were attached to it
            clusterMap.executeOnEntries(new InvalidateSessionAttributesEntryProcessor(session.getId()));
        }
    }

    private IMap<String, Object> getClusterMap() {
        return hazelcastInstance.getMap(clusterMapName);
    }

    private HazelcastHttpSession getSessionWithId(final String sessionId) {
        HazelcastHttpSession session = sessions.get(sessionId);
        if (session != null && !session.isValid()) {
            destroySession(session, true);
            session = null;
        }
        return session;
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
        if (sessionCookieHttpOnly) {
            try {
                sessionCookie.setHttpOnly(true);
            } catch (NoSuchMethodError e) {
                LOGGER.info("HttpOnly cookies require a Servlet 3.0+ container. Add the following to the "
                        + getClass().getName() + " mapping in web.xml to disable HttpOnly cookies:\n"
                        + "<init-param>\n"
                        + "    <param-name>cookie-http-only</param-name>\n"
                        + "    <param-value>false</param-value>\n"
                        + "</init-param>");
            }
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
                LOGGER.finest("Request is instance of RequestWrapper! Continue...");
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
            if (existingReq != null) {
                return;
            }
            HazelcastHttpSession session = reqWrapper.getSession(false);
            if (session != null && session.isValid()) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("UPDATING SESSION " + session.getId());
                }
                session.sessionDeferredWrite();
            }
        }
    }

    public final void destroy() {
        sessions.clear();
        originalSessions.clear();
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

    private static class ResponseWrapper extends HttpServletResponseWrapper {

        public ResponseWrapper(final HttpServletResponse original) {
            super(original);
        }
    }

    private static class LocalCacheEntry {
        volatile boolean dirty;
        volatile boolean reload;
        boolean removed;
        private Object value;
    }

    private class RequestWrapper extends HttpServletRequestWrapper {
        final ResponseWrapper res;
        HazelcastHttpSession hazelcastSession;
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
                public void forward(ServletRequest servletRequest, ServletResponse servletResponse)
                        throws ServletException, IOException {
                    original.getRequestDispatcher(path).forward(servletRequest, servletResponse);
                }

                public void include(ServletRequest servletRequest, ServletResponse servletResponse)
                        throws ServletException, IOException {
                    original.getRequestDispatcher(path).include(servletRequest, servletResponse);
                }
            };
        }

        public HazelcastHttpSession fetchHazelcastSession(boolean create) {
            if (requestedSessionId == null) {
                requestedSessionId = getSessionCookie(this);
                if (requestedSessionId == null) {
                    requestedSessionId = getParameter(HAZELCAST_SESSION_COOKIE_NAME);
                }
            }

            if (requestedSessionId != null) {
                hazelcastSession = getSessionWithId(requestedSessionId);
                if (hazelcastSession == null) {
                    final Boolean existing = (Boolean) getClusterMap()
                            .executeOnKey(requestedSessionId, new ReferenceSessionEntryProcessor());
                    if (existing != null && existing && create) {
                        // we already have the session in the cluster, so "copy" it to this node
                        hazelcastSession = createNewSession(RequestWrapper.this, requestedSessionId);
                    }
                }
            }

            return hazelcastSession;
        }

        @Override
        public HttpSession getSession() {
            return getSession(true);
        }

        @Override
        public HazelcastHttpSession getSession(final boolean create) {
            if (hazelcastSession != null && !hazelcastSession.isValid()) {
                LOGGER.finest("Session is invalid!");
                destroySession(hazelcastSession, true);
                hazelcastSession = null;
            } else if (hazelcastSession != null) {
                return hazelcastSession;
            }

            HttpSession originalSession = getOriginalSession(false);
            if (originalSession != null) {
                String hazelcastSessionId = originalSessions.get(originalSession.getId());
                if (hazelcastSessionId != null) {
                    hazelcastSession = sessions.get(hazelcastSessionId);
                    return hazelcastSession;
                }
                originalSessions.remove(originalSession.getId());
                originalSession.invalidate();
            }

            hazelcastSession = fetchHazelcastSession(create);
            if (hazelcastSession == null && create) {
                hazelcastSession = createNewSession(RequestWrapper.this, null);
            }
            if (deferredWrite) {
                prepareReloadingSession(hazelcastSession);
            }
            return hazelcastSession;
        }
    } // END of RequestWrapper

    private class HazelcastHttpSession implements HttpSession {

        volatile boolean valid = true;
        final String id;
        final HttpSession originalSession;
        private final Map<String, LocalCacheEntry> localCache;
        private final boolean deferredWrite;
        // only true if session is created first time in the cluster
        private volatile boolean clusterWideNew;

        public HazelcastHttpSession(final String sessionId, final HttpSession originalSession,
                                    final boolean deferredWrite) {
            this.id = sessionId;
            this.originalSession = originalSession;
            this.deferredWrite = deferredWrite;
            this.localCache = deferredWrite ? buildLocalCache() : null;
        }

        public Object getAttribute(final String name) {
            IMap<String, Object> clusterMap = getClusterMap();
            if (deferredWrite) {
                LocalCacheEntry cacheEntry = localCache.get(name);
                if (cacheEntry == null || (cacheEntry.reload && !cacheEntry.dirty)) {
                    Object value = clusterMap.get(buildAttributeName(name));
                    if (value == null) {
                        cacheEntry = NULL_ENTRY;
                    } else {
                        cacheEntry = new LocalCacheEntry();
                        cacheEntry.value = value;
                        cacheEntry.reload = false;
                    }
                    localCache.put(name, cacheEntry);
                }
                return cacheEntry != NULL_ENTRY ? cacheEntry.value : null;
            }
            return clusterMap.get(buildAttributeName(name));
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
            return servletContext;
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
            originalSession.invalidate();
            destroySession(this, true);
        }

        public boolean isNew() {
            return originalSession.isNew() && clusterWideNew;
        }

        public void putValue(final String name, final Object value) {
            setAttribute(name, value);
        }

        public void removeAttribute(final String name) {
            if (deferredWrite) {
                LocalCacheEntry entry = localCache.get(name);
                if (entry != null && entry != NULL_ENTRY) {
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
                removeAttribute(name);
                return;
            }
            if (deferredWrite) {
                LocalCacheEntry entry = localCache.get(name);
                if (entry == null || entry == NULL_ENTRY) {
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

        /**
         * @return {@code true} if {@link #deferredWrite} is enabled <i>and</i> at least one entry in the local
         *         cache is dirty; otherwise, {@code false}
         */
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

        private Map<String, LocalCacheEntry> buildLocalCache() {
            Set<Entry<String, Object>> entrySet = getClusterMap().entrySet(new SessionAttributePredicate(id));

            Map<String, LocalCacheEntry> cache = new ConcurrentHashMap<String, LocalCacheEntry>();
            for (Entry<String, Object> entry : entrySet) {
                String attributeKey = extractAttributeKey(entry.getKey());
                LocalCacheEntry cacheEntry = cache.get(attributeKey);
                if (cacheEntry == null) {
                    cacheEntry = new LocalCacheEntry();
                    cache.put(attributeKey, cacheEntry);
                }
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Storing " + attributeKey + " on session " + id);
                }
                cacheEntry.value = entry.getValue();
                cacheEntry.dirty = false;
            }

            return cache;
        }

        private void sessionDeferredWrite() {
            if (sessionChanged()) {
                IMap<String, Object> clusterMap = getClusterMap();

                Iterator<Entry<String, LocalCacheEntry>> iterator = localCache.entrySet().iterator();
                while (iterator.hasNext()) {
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
        }

        private Set<String> selectKeys() {
            Set<String> keys = new HashSet<String>();
            if (!deferredWrite) {
                for (String qualifiedAttributeKey : getClusterMap().keySet(new SessionAttributePredicate(id))) {
                    keys.add(extractAttributeKey(qualifiedAttributeKey));
                }
            } else {
                for (Entry<String, LocalCacheEntry> entry : localCache.entrySet()) {
                    if (!entry.getValue().removed && entry.getValue() != NULL_ENTRY) {
                        keys.add(entry.getKey());
                    }
                }
            }
            return keys;
        }

        public void setClusterWideNew(boolean clusterWideNew) {
            this.clusterWideNew = clusterWideNew;
        }
    } // END of HazelSession
} // END of WebFilter
