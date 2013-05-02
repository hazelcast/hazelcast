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
import com.hazelcast.core.*;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.web.HazelcastInstanceLoader.*;

public class WebFilter implements Filter {

    private static final ILogger logger = Logger.getLogger(WebFilter.class.getName());

    private static final String HAZELCAST_REQUEST = "*hazelcast-request";

    private static final String HAZELCAST_SESSION_COOKIE_NAME = "hazelcast.sessionId";

    private static final ConcurrentMap<String, String> mapOriginalSessions = new ConcurrentHashMap<String, String>(1000);

    private static final ConcurrentMap<String, HazelcastHttpSession> mapSessions = new ConcurrentHashMap<String, HazelcastHttpSession>(1000);

    private HazelcastInstanceImpl hazelcastInstance;

    private String clusterMapName = "none";

    private String sessionCookieName = HAZELCAST_SESSION_COOKIE_NAME;

    private String sessionCookieDomain = null;

    private boolean sessionCookieSecure = false;

    private boolean sessionCookieHttpOnly = false;

    private boolean stickySession = true;

    private boolean debug = false;

    private boolean shutdownOnDestroy = true;

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
        String debugParam = getParam("debug");
        if (debugParam != null) {
            debug = Boolean.valueOf(debugParam);
        }
        String mapName = getParam("map-name");
        if (mapName != null) {
            clusterMapName = mapName;
        } else {
            clusterMapName = "_web_" + servletContext.getServletContextName();
        }
        Config hzConfig = hazelcastInstance.getConfig();
        String sessionTTL = getParam("session-ttl-seconds");
        if (sessionTTL != null) {
            MapConfig mapConfig = new MapConfig(clusterMapName);
            mapConfig.setTimeToLiveSeconds(Integer.valueOf(sessionTTL));
            hzConfig.addMapConfig(mapConfig);
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
        if (!stickySession) {
            getClusterMap().addEntryListener(new EntryListener() {
                public void entryAdded(EntryEvent entryEvent) {
                }

                public void entryRemoved(EntryEvent entryEvent) {
                    if (entryEvent.getMember() == null || // client events has no owner member
                            !entryEvent.getMember().localMember()) {
                        removeSessionLocally((String) entryEvent.getKey());
                    }
                }

                public void entryUpdated(EntryEvent entryEvent) {
                    if (entryEvent.getMember() == null || // client events has no owner member
                            !entryEvent.getMember().localMember()) {
                        markSessionDirty((String) entryEvent.getKey());
                    }
                }

                public void entryEvicted(EntryEvent entryEvent) {
                    entryRemoved(entryEvent);
                }
            }, false);
        }
        log("sticky:" + stickySession + ", debug: " + debug + ", shutdown-on-destroy: " + shutdownOnDestroy
                + ", map-name: " + clusterMapName);
    }

    private void initInstance() throws ServletException {
        if (properties == null) {
            properties = new Properties();
        }
        setProperty(CONFIG_LOCATION);
        setProperty(INSTANCE_NAME);
        setProperty(USE_CLIENT);
        setProperty(CLIENT_CONFIG_LOCATION);
        hazelcastInstance = (HazelcastInstanceImpl) getInstance(properties);
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
            log("Destroying session locally " + hazelSession);
            hazelSession.destroy();
        }
    }

    private void markSessionDirty(String sessionId) {
        HazelcastHttpSession hazelSession = mapSessions.get(sessionId);
        if (hazelSession != null) {
            hazelSession.setDirty(true);
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

    protected void log(final Object obj) {
        Level level = Level.FINEST;
        if (debug) {
            level = Level.INFO;
        }
        logger.log(level, obj.toString());
    }

    private HazelcastHttpSession createNewSession(RequestWrapper requestWrapper, String existingSessionId) {
        String id = existingSessionId != null ? existingSessionId : generateSessionId();
        if (requestWrapper.getOriginalSession(false) != null) {
            log("Original session exists!!!");
        }
        HttpSession originalSession = requestWrapper.getOriginalSession(true);
        HazelcastHttpSession hazelcastSession = new HazelcastHttpSession(WebFilter.this, id, originalSession);
        mapSessions.put(hazelcastSession.getId(), hazelcastSession);
        String oldHazelcastSessionId = mapOriginalSessions.put(originalSession.getId(), hazelcastSession.getId());
        if (oldHazelcastSessionId != null) {
            log("!!! Overriding an existing hazelcastSessionId " + oldHazelcastSessionId);
        }
        log("Created new session with id: " + id);
        log(mapSessions.size() + " is sessions.size and originalSessions.size: " + mapOriginalSessions.size());
        addSessionCookie(requestWrapper, id);
        return hazelcastSession;
    }

    /**
     * Destroys a session, determining if it should be destroyed clusterwide automatically or via expiry.
     *
     * @param session             The session to be destroyed
     * @param removeGlobalSession boolean value - true if the session should be destroyed irrespective of active time
     */
    private void destroySession(HazelcastHttpSession session, boolean removeGlobalSession) {
        log("Destroying local session: " + session.getId());
        mapSessions.remove(session.getId());
        mapOriginalSessions.remove(session.originalSession.getId());
        session.destroy();
        if (removeGlobalSession) {
            log("Destroying cluster session: " + session.getId() + " => Ignore-timeout: true");
            getClusterMap().remove(session.getId());
        }
    }

    private IMap getClusterMap() {
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
//                    original.getRequestDispatcher(path).forward(original, servletResponse);
                    original.getRequestDispatcher(path).forward(RequestWrapper.this, servletResponse);
                }

                public void include(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
//                    original.getRequestDispatcher(path).include(original, servletResponse);
                    original.getRequestDispatcher(path).include(RequestWrapper.this, servletResponse);
                }
            };
        }

        public String fetchHazelcastSessionId() {
            if (requestedSessionId != null) {
                return requestedSessionId;
            }
            requestedSessionId = getSessionCookie(this);
            return requestedSessionId;
        }

        @Override
        public HttpSession getSession() {
            return getSession(true);
        }

        @Override
        public HazelcastHttpSession getSession(final boolean create) {
            if (hazelcastSession != null && !hazelcastSession.isValid()) {
                log("Session is invalid!");
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
                    } else if (hazelcastSession.isDirty()) {
                        hazelcastSession = null;
                    }
                }
            }
            if (hazelcastSession != null)
                return hazelcastSession;
            final String requestedSessionId = fetchHazelcastSessionId();
            if (requestedSessionId != null) {
                hazelcastSession = getSessionWithId(requestedSessionId);
                if (hazelcastSession == null) {
                    final Map mapSession = (Map) getClusterMap().get(requestedSessionId);
                    if (mapSession != null) {
                        // we already have the session in the cluster
                        // loading it...
                        hazelcastSession = createNewSession(RequestWrapper.this, requestedSessionId);
                        overrideSession(hazelcastSession, mapSession);
                    }
                }
            }
            if (hazelcastSession == null && create) {
                hazelcastSession = createNewSession(RequestWrapper.this, null);
            } else if (hazelcastSession != null && !stickySession && requestedSessionId != null && hazelcastSession.isDirty()) {
                log(requestedSessionId + " is dirty reloading.");
                final Map mapSession = (Map) getClusterMap().get(requestedSessionId);
                overrideSession(hazelcastSession, mapSession);
            }
            return hazelcastSession;
        }

        private void overrideSession(HazelcastHttpSession session, Map mapSession) {
            if (session == null || mapSession == null) return;
            final Enumeration<String> atts = session.getAttributeNames();
            while (atts.hasMoreElements()) {
                session.removeAttribute(atts.nextElement());
            }
            Map mapData = null;
            final Set<Map.Entry> entries = mapSession.entrySet();
            for (final Map.Entry entry : entries) {
                session.setAttribute((String) entry.getKey(), entry.getValue());
                if (mapData == null) {
                    mapData = new HashMap<String, Object>();
                }
                mapData.put(entry.getKey(), entry.getValue());
            }
            session.sessionChanged(session.writeObject(mapData));
            session.setDirty(false);
        }
    } // END of RequestWrapper

    private class ResponseWrapper extends HttpServletResponseWrapper {

        RequestWrapper req = null;

        public ResponseWrapper(final HttpServletResponse original) {
            super(original);
        }

        public void setRequest(final RequestWrapper req) {
            this.req = req;
        }
    }

    private class HazelcastHttpSession implements HttpSession {
        private Data currentSessionData = null;

        volatile boolean valid = true;

        volatile boolean dirty = false;

        final String id;

        final HttpSession originalSession;

        final WebFilter webFilter;

        private final IAtomicLong timestamp;

        public HazelcastHttpSession(WebFilter webFilter, final String sessionId, HttpSession originalSession) {
            this.webFilter = webFilter;
            this.id = sessionId;
            this.originalSession = originalSession;
            timestamp = hazelcastInstance.getAtomicLong(clusterMapName + "_" + id);
        }

        public Object getAttribute(final String name) {
            return originalSession.getAttribute(name);
        }

        public Enumeration getAttributeNames() {
            return originalSession.getAttributeNames();
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
            return originalSession.getValueNames();
        }

        public boolean isDirty() {
            return dirty;
        }

        public void setDirty(boolean dirty) {
            this.dirty = dirty;
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
            originalSession.removeAttribute(name);
        }

        public void setAttribute(final String name, final Object value) {
            if (value != null && !(value instanceof Serializable)) {
                throw new IllegalArgumentException(new NotSerializableException(value.getClass().getName()));
            }
            originalSession.setAttribute(name, value);
        }

        public void removeValue(final String name) {
            removeAttribute(name);
        }

        public boolean sessionChanged(final Data data) {
            try {
                if (data == null) {
                    return currentSessionData != null;
                }
                return currentSessionData == null || !data.equals(currentSessionData);
            } finally {
                currentSessionData = data;
            }
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

        public synchronized Data writeObject(final Object obj) {
            if (obj == null)
                return null;
            return hazelcastInstance.node.serializationService.toData(obj);
        }

        void destroy() {
            valid = false;
            timestamp.destroy();
        }

        public boolean isValid() {
            return valid;
        }

        void setAccessed() {
            timestamp.set(Clock.currentTimeMillis());
        }

        long getLastAccessed() {
            return hazelcastInstance.getLifecycleService().isRunning()
                    ? timestamp.get() : 0L;
        }
    }// END of HazelSession

    private static synchronized String generateSessionId() {
        final String id = UUID.randomUUID().toString();
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
        sessionCookie.setHttpOnly(sessionCookieHttpOnly);
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
                log("Request is instance of RequestWrapper! Continue...");
                chain.doFilter(req, res);
                return;
            }
            HttpServletRequest httpReq = (HttpServletRequest) req;
            RequestWrapper existingReq = (RequestWrapper) req.getAttribute(HAZELCAST_REQUEST);
            final ResponseWrapper resWrapper = new ResponseWrapper((HttpServletResponse) res);
            final RequestWrapper reqWrapper = new RequestWrapper(httpReq, resWrapper);
            resWrapper.setRequest(reqWrapper);
            if (existingReq != null) {
                reqWrapper.setHazelcastSession(existingReq.hazelcastSession, existingReq.requestedSessionId);
            }
            req = null;
            res = null;
            httpReq = null;
            chain.doFilter(reqWrapper, resWrapper);
            if (existingReq != null) return;
            req = null; // for easy debugging. reqWrapper should be used
            HazelcastHttpSession session = reqWrapper.getSession(false);
            if (session != null && session.isValid()) {
                session.setAccessed();
                final Enumeration<String> attNames = session.getAttributeNames();
                Map mapData = null;
                while (attNames.hasMoreElements()) {
                    final String attName = attNames.nextElement();
                    final Object value = session.getAttribute(attName);
                    if (mapData == null) {
                        mapData = new HashMap<String, Object>();
                    }
                    mapData.put(attName, value);
                }
                Data data = session.writeObject(mapData);
                boolean sessionChanged = session.sessionChanged(data);
                if (sessionChanged) {
                    if (data == null) {
                        mapData = new HashMap<String, Object>();
                        data = session.writeObject(mapData);
                    }
                    log("PUTTING SESSION " + session.getId());
                    getClusterMap().put(session.getId(), data);
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

