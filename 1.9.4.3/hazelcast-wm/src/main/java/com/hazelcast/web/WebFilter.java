/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.web;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.nio.Data;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebFilter implements Filter {

    private static final ConcurrentMap<String, String> mapOriginalSessions = new ConcurrentHashMap<String, String>(1000);

    private static final ConcurrentMap<String, HazelcastHttpSession> mapSessions = new ConcurrentHashMap<String, HazelcastHttpSession>(1000);

    private ServletContext servletContext = null;

    private String clusterMapName = "none";

    private boolean stickySession = true;

    private static Logger logger = Logger.getLogger(WebFilter.class.getName());

    private boolean DEBUG = false;

    private static final String HAZELCAST_REQUEST = "*hazelcast-request";

    private static final String HAZELCAST_SESSION_COOKIE_NAME = "hazelcast.sessionId";

    public WebFilter() {
    }

    public void init(final FilterConfig config) throws ServletException {
        String debugParam = config.getInitParameter("debug");
        if (debugParam != null) {
            DEBUG = Boolean.valueOf(debugParam);
        }
        servletContext = config.getServletContext();
        String mapName = config.getInitParameter("map-name");
        if (mapName != null) {
            clusterMapName = mapName;
        } else {
            clusterMapName = "_web_" + servletContext.getServletContextName();
        }
        String stickySessionParam = config.getInitParameter("sticky-session");
        if (stickySessionParam != null) {
            stickySession = Boolean.valueOf(stickySessionParam);
        }
        if (!stickySession) {
            getClusterMap().addEntryListener(new EntryListener() {
                public void entryAdded(EntryEvent entryEvent) {
                }

                public void entryRemoved(EntryEvent entryEvent) {
                    removeSessionLocally((String) entryEvent.getKey());
                }

                public void entryUpdated(EntryEvent entryEvent) {
                    markSessionDirty((String) entryEvent.getKey());
                }

                public void entryEvicted(EntryEvent entryEvent) {
                }
            }, false);
        }
        log("sticky:" + stickySession + ", debug: " + DEBUG + ", map-name: " + clusterMapName);
    }

    void removeSessionLocally(String sessionId) {
        HazelcastHttpSession hazelSession = mapSessions.remove(sessionId);
        if (hazelSession != null) {
            mapOriginalSessions.remove(hazelSession.originalSession.getId());
            log("Destroying session locally " + hazelSession);
            hazelSession.destroy();
        }
    }

    void markSessionDirty(String sessionId) {
        HazelcastHttpSession hazelSession = mapSessions.get(sessionId);
        if (hazelSession != null) {
            hazelSession.setDirty(true);
        }
    }

    public static void destroyOriginalSession(HttpSession originalSession) {
        String hazelcastSessionId = mapOriginalSessions.remove(originalSession.getId());
        if (hazelcastSessionId != null) {
            HazelcastHttpSession hazelSession = mapSessions.remove(hazelcastSessionId);
            if (hazelSession != null) {
                hazelSession.webFilter.destroySession(hazelSession);
            }
        }
    }

    void log(final Object obj) {
        if (DEBUG) {
            logger.log(Level.FINEST, obj.toString());
            System.out.println(obj.toString());
        }
    }

    HazelcastHttpSession createNewSession(RequestWrapper requestWrapper, String existingSessionId) {
        String id = existingSessionId != null ? existingSessionId : generateSessionId();
        if (requestWrapper.getOriginalSession(false) != null) {
            log("Original session exist!!!");
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

    void destroySession(final HazelcastHttpSession session) {
        log("Destroying " + session.getId());
        mapSessions.remove(session.getId());
        mapOriginalSessions.remove(session.originalSession.getId());
        session.destroy();
        getClusterMap().remove(session.getId());
    }

    public IMap getClusterMap() {
        return Hazelcast.getMap(clusterMapName);
    }

    HazelcastHttpSession getSessionWithId(final String sessionId) {
        HazelcastHttpSession session = mapSessions.get(sessionId);
        if (session != null && !session.isValid()) {
            session = null;
            destroySession(session);
        }
        return session;
    }

    class RequestWrapper extends HttpServletRequestWrapper {
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
                    original.getRequestDispatcher(path).forward(original, servletResponse);
                }

                public void include(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
                    original.getRequestDispatcher(path).include(original, servletResponse);
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
                destroySession(hazelcastSession);
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
            final Set<Map.Entry> entries = mapSession.entrySet();
            for (final Map.Entry entry : entries) {
                session.setAttribute((String) entry.getKey(), entry.getValue());
            }
            session.setDirty(false);
        }
    } // END of RequestWrapper

    class ResponseWrapper extends HttpServletResponseWrapper {

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

        public HazelcastHttpSession(WebFilter webFilter, final String sessionId, HttpSession originalSession) {
            this.webFilter = webFilter;
            this.id = sessionId;
            this.originalSession = originalSession;
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
            return originalSession.getValue(name);
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
            destroySession(this);
        }

        public boolean isNew() {
            return originalSession.isNew();
        }

        public void putValue(final String name, final Object value) {
            originalSession.setAttribute(name, value);
        }

        public void removeAttribute(final String name) {
            originalSession.removeAttribute(name);
        }

        public void setAttribute(final String name, final Object value) {
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
                if (currentSessionData == null) {
                    return true;
                }
                return !data.equals(currentSessionData);
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
            return ThreadContext.get().toData(obj);
        }

        void destroy() {
            valid = false;
        }

        public boolean isValid() {
            return valid;
        }
    }// END of HazelSession

    private static synchronized String generateSessionId() {
        String id = UUID.randomUUID().toString();
        final StringBuilder sb = new StringBuilder();
        final char[] chars = id.toCharArray();
        for (final char c : chars) {
            if (c != '-') {
                if (Character.isLetter(c)) {
                    sb.append(Character.toUpperCase(c));
                } else
                    sb.append(c);
            }
        }
        return id = "HZ" + sb.toString();
    }

    private void addSessionCookie(final RequestWrapper req, final String sessionId) {
        final Cookie sessionCookie = new Cookie(HAZELCAST_SESSION_COOKIE_NAME, sessionId);
        sessionCookie.setPath(req.getContextPath());
        sessionCookie.setMaxAge(-1);
        req.res.addCookie(sessionCookie);
    }

    private static String getSessionCookie(final RequestWrapper req) {
        final Cookie[] cookies = req.getCookies();
        if (cookies != null) {
            for (final Cookie cookie : cookies) {
                final String name = cookie.getName();
                final String value = cookie.getValue();
                if (name.equalsIgnoreCase(HAZELCAST_SESSION_COOKIE_NAME)) {
                    return value;
                }
            }
        }
        return null;
    }

    public void doFilter(ServletRequest req, ServletResponse res, final FilterChain chain)
            throws IOException, ServletException {
        log("FILTERING " + req.getClass().getName());
        log(mapSessions.size() + " FILTERING " + mapOriginalSessions.size());
        if (!(req instanceof HttpServletRequest)) {
            chain.doFilter(req, res);
        } else {
            if (req instanceof RequestWrapper) {
                log("Request is instance ! continue...");
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
                final Enumeration<String> attNames = session.getAttributeNames();
                Map mapData = null;
                while (attNames.hasMoreElements()) {
                    final String attName = attNames.nextElement();
                    final Object value = session.getAttribute(attName);
                    if (value instanceof Serializable) {
                        if (mapData == null) {
                            mapData = new HashMap<String, Object>();
                        }
                        mapData.put(attName, value);
                    }
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

    public void destroy() {
        mapSessions.clear();
        mapOriginalSessions.clear();
        Hazelcast.getLifecycleService().shutdown();
    }
}// END of WebFilter

