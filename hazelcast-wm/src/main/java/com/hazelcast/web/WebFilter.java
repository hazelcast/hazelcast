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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebFilter implements Filter {

    private static final ConcurrentMap<String, String> mapOriginalSessions = new ConcurrentHashMap<String, String>(1000);

    private static final ConcurrentMap<String, HazelSession> mapSessions = new ConcurrentHashMap<String, HazelSession>(1000);

    private ServletContext servletContext = null;

    private String clusterMapName = "none";

    private int maxInactiveInterval = 30; // minutes

    private static Logger logger = Logger.getLogger(WebFilter.class.getName());

    private static final boolean DEBUG = false;

    private static final String SESSION_URL_PHRASE = ";jsessionid=";

    private static final String HAZELCAST_REQUEST = "*hazelcast-request";

    public WebFilter() {
    }

    public void init(final FilterConfig config) throws ServletException {
        final String sessionTimeoutValue = config.getInitParameter("session-timeout");
        servletContext = config.getServletContext();
        if (sessionTimeoutValue != null) {
            maxInactiveInterval = Integer.parseInt(sessionTimeoutValue.trim());
        }
        clusterMapName = "_web_" + servletContext.getServletContextName();
    }

    public static void destroySession(HttpSession originalSession) {
        String hazelcastSessionId = mapOriginalSessions.remove(originalSession.getId());
        if (hazelcastSessionId != null) {
            HazelSession hazelSession = mapSessions.remove(hazelcastSessionId);
            if (hazelSession != null) {
                log("Destroying session " + hazelSession);
                hazelSession.webFilter.destroySession(hazelSession);
            }
        }
    }

    static void log(final Object obj) {
        if (DEBUG) {
            logger.log(Level.FINEST, obj.toString());
            System.out.println(obj.toString());
        }
    }

    boolean urlRewriteEnabled() {
        return true;
    }

    void changeSessionId(final HazelSession session) {
        String oldId = session.getId();
        mapSessions.remove(oldId);
        getClusterMap().remove(oldId);
        session.id = generateSessionId();
        while (mapSessions.containsKey(session.getId())) {
            session.id = generateSessionId();
        }
        mapSessions.put(session.getId(), session);
    }

    HazelSession createNewSession(String requestedSessionId) {
        String id = (requestedSessionId == null) ? generateSessionId() : requestedSessionId;
        while (mapSessions.containsKey(id)) {
            id = generateSessionId();
        }
        return getSessionWithId(id, true);
    }

    void destroySession(final HazelSession session) {
        final String id = session.id;
        session.destroy();
        mapSessions.remove(id);
        log(id + " Removing from cluster " + getClusterMap().remove(id));
    }

    public IMap getClusterMap() {
        return Hazelcast.getMap(clusterMapName);
    }

    HazelSession getSessionWithId(final String sessionId, final boolean create) {
        HazelSession session = mapSessions.get(sessionId);
        if (session == null && create) {
            session = new HazelSession(this, sessionId);
            session.setMaxInactiveInterval(maxInactiveInterval * 60);
            final HazelSession oldSessionInfo = mapSessions.putIfAbsent(sessionId, session);
            if (oldSessionInfo != null) {
                session = oldSessionInfo;
            }
        }
        return session;
    }

    static class IteratorEnumeration implements Enumeration<String> {
        Iterator<String> it = null;

        IteratorEnumeration(final Iterator<String> it) {
            this.it = it;
        }

        public boolean hasMoreElements() {
            return it != null && it.hasNext();
        }

        public String nextElement() {
            if (it == null)
                return null;
            return it.next();
        }
    }

    class RequestWrapper extends HttpServletRequestWrapper {
        HazelSession hazelSession = null;

        final ResponseWrapper res;

        final ConcurrentMap<String, Object> atts = new ConcurrentHashMap<String, Object>();

        final long creationTime;

        String requestedSessionId = null;

        boolean requestedSessionIdValid = true;

        boolean requestedSessionIdFromCookie = false;

        boolean requestedSessionIdFromURL = false;

        public RequestWrapper(final HttpServletRequest req,
                              final ResponseWrapper res) {
            super(req);
            log("REQ Wrapping " + req.getClass().getName());
            this.res = res;
            req.setAttribute(HAZELCAST_REQUEST, this);
            creationTime = System.nanoTime();
        }

        void setExtractSessionId() {
            final Cookie[] cookies = ((HttpServletRequest) getRequest()).getCookies();
            if (cookies != null) {
                for (final Cookie cookie : cookies) {
                    if (cookie.getName().equalsIgnoreCase("JSESSIONID")) {
                        requestedSessionId = cookie.getValue();
                        requestedSessionIdFromCookie = true;
                        if (DEBUG) {
                            log("Extracted sessionId from cookie " + requestedSessionId);
                        }
                    }
                }
            }
            if (requestedSessionId == null) {
                logger.log(Level.FINEST, "contextPath : " + getContextPath());
                logger.log(Level.FINEST, "queryString : " + getQueryString());
                logger.log(Level.FINEST, "requestURI : " + getRequestURI());
                logger.log(Level.FINEST, "requestURL : " + getRequestURL());
                requestedSessionId = res.extractSessionId(getRequestURL().toString());
                if (DEBUG) {
                    log("Extracted sessionId from URL " + requestedSessionId);
                }
                if (requestedSessionId != null) {
                    requestedSessionIdFromURL = true;
                }
            }
        }

        public void setRequestedSessionId(HazelSession hazelSession, String requestedSessionId, boolean fromCookie) {
            this.hazelSession = hazelSession;
            this.requestedSessionId = requestedSessionId;
            requestedSessionIdFromCookie = fromCookie;
            requestedSessionIdFromURL = !requestedSessionIdFromCookie;
        }

        @Override
        public RequestDispatcher getRequestDispatcher(final String path) {
            final ServletRequest original = getRequest();
            return new RequestDispatcher() {
                public void forward(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
                    log("FORWARD " + original);
                    original.getRequestDispatcher(path).forward(original, servletResponse);
                }

                public void include(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
                    original.getRequestDispatcher(path).include(original, servletResponse);
                }
            };
        }

        @Override
        public Enumeration getAttributeNames() {
            if (atts.size() == 0)
                return new IteratorEnumeration(null);
            return new IteratorEnumeration(atts.keySet().iterator());
        }

        @Override
        public String getRequestedSessionId() {
            if (requestedSessionId != null)
                return requestedSessionId;
            else
                return super.getRequestedSessionId();
        }

        @Override
        public HttpSession getSession() {
            return getSession(true);
        }

        @Override
        public HazelSession getSession(final boolean create) {
            if (hazelSession != null)
                return hazelSession;
            final String requestedSessionId = getRequestedSessionId();
            HazelSession session = null;
            if (requestedSessionId != null) {
                session = getSessionWithId(requestedSessionId, false);
            }
            log(requestedSessionId + " is requestedSessionId and  getSession : " + session);
            if (session == null) {
                if (create) {
                    HttpSession originalSession = super.getSession(true);
                    session = createNewSession(requestedSessionId);
                    session.setOriginalSession(originalSession);
                    hazelSession = session;
                    if (requestedSessionId != null) {
                        final Map mapSession = (Map) getClusterMap().remove(requestedSessionId);
                        log(session + " Reloading from map.. " + mapSession);
                        log("ContextPath " + getContextPath());
                        log("pathInfo " + getPathInfo());
                        log("pathtranslated " + getPathTranslated());
                        log("requesturi " + getRequestURI());
                        if (mapSession != null) {
                            final Set<Map.Entry> entries = mapSession.entrySet();
                            for (final Map.Entry entry : entries) {
                                session.setAttribute((String) entry.getKey(), entry.getValue());
                            }
                        }
                        removeCookieForSession(this, requestedSessionId);
                        final Cookie[] cookies = getCookies();
                        if (cookies != null) {
                            removeCookies:
                            for (final Cookie cookie : cookies) {
                                final String name = cookie.getName();
                                final String value = cookie.getValue();
                                final String path = cookie.getPath();
                                if (getContextPath().equals(path)) {
                                    if (name.equals("JSESSIONID")
                                            && value.equals(requestedSessionId)) {
                                        if (DEBUG) {
                                            log("Found old sessionId cookie DELETING " + value);
                                        }
                                        cookie.setMaxAge(0);
                                        res.addCookie(cookie);
                                        break removeCookies;
                                    }
                                }
                            }
                        }
                    }
                    addCookieForSession(this, session.getId());
                }
            }
            return session;
        }

        @Override
        public boolean isRequestedSessionIdFromCookie() {
            return requestedSessionIdFromCookie;
        }

        @Override
        public boolean isRequestedSessionIdFromUrl() {
            return isRequestedSessionIdFromURL();
        }

        @Override
        public boolean isRequestedSessionIdFromURL() {
            return requestedSessionIdFromURL;
        }

        @Override
        public boolean isRequestedSessionIdValid() {
            return requestedSessionIdValid;
        }

        public void setRequestedSessionIdValid(final boolean valid) {
            requestedSessionIdValid = valid;
        }
    } // END of RequestWrapper

    class ResponseWrapper extends HttpServletResponseWrapper {

        RequestWrapper req = null;

        public ResponseWrapper(final HttpServletResponse original) {
            super(original);
        }

        @Override
        public String encodeURL(final String url) {
            if (url == null) {
                throw new NullPointerException("URL can not be null");
            }
            if (!urlRewriteEnabled()) {
                return url;
            }
            return encodeURL(url, SESSION_URL_PHRASE);
        }

        public String extractSessionId(final String url) {
            final int prefix = url.indexOf(SESSION_URL_PHRASE);
            if (prefix != -1) {
                final int start = prefix + SESSION_URL_PHRASE.length();
                int suffix = url.indexOf("?", start);
                if (suffix < 0)
                    suffix = url.indexOf("#", start);
                if (suffix <= prefix)
                    return url.substring(start);
                return url.substring(start, suffix);
            }
            return null;
        }

        public RequestWrapper getRequest() {
            return req;
        }

        public void setRequest(final RequestWrapper req) {
            this.req = req;
        }

        private String encodeURL(final String url, final String sessionURLPhrase) {
            if (url == null) {
                throw new NullPointerException("URL can not be null");
            }
            // should not encode if cookies in evidence
            if (url != null || req == null || req.isRequestedSessionIdFromCookie()) {
                final int prefix = url.indexOf(sessionURLPhrase);
                if (prefix != -1) {
                    int suffix = url.indexOf("?", prefix);
                    if (suffix < 0)
                        suffix = url.indexOf("#", prefix);
                    if (suffix <= prefix)
                        return url.substring(0, prefix);
                    return url.substring(0, prefix) + url.substring(suffix);
                }
                return url;
            }
            final HazelSession session = req.getSession(false);
            if (session == null)
                return url;
            if (!session.valid.get())
                return url;
            final String id = session.getId();
            final int prefix = url.indexOf(sessionURLPhrase);
            if (prefix != -1) {
                int suffix = url.indexOf("?", prefix);
                if (suffix < 0)
                    suffix = url.indexOf("#", prefix);
                if (suffix <= prefix)
                    return url.substring(0, prefix + sessionURLPhrase.length()) + id;
                return url.substring(0, prefix + sessionURLPhrase.length()) + id
                        + url.substring(suffix);
            }
            // edit the session
            int suffix = url.indexOf('?');
            if (suffix < 0)
                suffix = url.indexOf('#');
            if (suffix < 0)
                return url + sessionURLPhrase + id;
            return url.substring(0, suffix) + sessionURLPhrase + id + url.substring(suffix);
        }
    }

    private class HazelSession implements HttpSession {
        private Data currentSessionData = null;

        public int minSize = -1;

        public int maxSize = -1;

        AtomicLong maxInactiveInterval = new AtomicLong(30 * 60 * 1000);

        AtomicLong creationTime = new AtomicLong();

        AtomicLong lastAccessedTime = new AtomicLong();

        AtomicBoolean valid = new AtomicBoolean(true);

        AtomicBoolean isNew = new AtomicBoolean(true);

        AtomicBoolean knownToCluster = new AtomicBoolean(false);

        String id = null;

        HttpSession originalSession;

        WebFilter webFilter;

        public HazelSession(WebFilter webFilter, final String sessionId) {
            this.webFilter = webFilter;
            this.id = sessionId;
            creationTime.set(System.currentTimeMillis());
            lastAccessedTime.set(System.currentTimeMillis());
        }

        public HttpSession getOriginalSession() {
            return originalSession;
        }

        public void setOriginalSession(HttpSession originalSession) {
            this.originalSession = originalSession;
            log(HazelSession.this + " setting original session " + originalSession);
            mapOriginalSessions.put(originalSession.getId(), id);
        }

        public boolean expired(final long currentTime) {
            final long maxInactive = maxInactiveInterval.get();
            if (maxInactive < 0)
                return false;
            return (currentTime - lastAccessedTime.get()) >= maxInactive;
        }

        public Object getAttribute(final String name) {
            checkState();
            return originalSession.getAttribute(name);
        }

        public Enumeration getAttributeNames() {
            checkState();
            return originalSession.getAttributeNames();
        }

        public long getCreationTime() {
            checkState();
            return creationTime.get();
        }

        public String getId() {
            checkState();
            return id;
        }

        public long getLastAccessedTime() {
            checkState();
            return lastAccessedTime.get();
        }

        /**
         * returns in seconds..
         */
        public int getMaxInactiveInterval() {
            return (int) (maxInactiveInterval.get() / 1000);
        }

        public ServletContext getServletContext() {
            return servletContext;
        }

        public HttpSessionContext getSessionContext() {
            checkState();
            return null;
        }

        public Object getValue(final String name) {
            checkState();
            return originalSession.getValue(name);
        }

        public String[] getValueNames() {
            checkState();
            return originalSession.getValueNames();
        }

        public void invalidate() {
            checkState();
            originalSession.invalidate();
            destroySession(this);
        }

        public boolean isNew() {
            checkState();
            return isNew.get();
        }

        public boolean knownToCluster() {
            return knownToCluster.get();
        }

        public void putValue(final String name, final Object value) {
            checkState();
            originalSession.setAttribute(name, value);
        }

        public void removeAttribute(final String name) {
            checkState();
            originalSession.removeAttribute(name);
        }

        public void setAttribute(final String name, final Object value) {
            checkState();
            originalSession.setAttribute(name, value);
        }

        public void removeValue(final String name) {
            checkState();
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

        public void setKnownToCluster(final boolean knownToCluster) {
            this.knownToCluster.set(knownToCluster);
        }

        public void setLastAccessed() {
            checkState();
            lastAccessedTime.set(System.currentTimeMillis());
        }

        public void setMaxInactiveInterval(int maxInactiveSeconds) {
            if (DEBUG)
                log("setting max interval seconds to " + maxInactiveSeconds);
            if (maxInactiveSeconds < 0)
                maxInactiveSeconds = -1;
            maxInactiveInterval.set(maxInactiveSeconds * 1000L);
        }

        public void setNew(final boolean isNew) {
            this.isNew.set(isNew);
        }

        public synchronized Data writeObject(final Object obj) {
            if (obj == null)
                return null;
            try {
                final Data data = ThreadContext.get().toData(obj);
                final int size = data.size();
                if (minSize == -1 || minSize > size)
                    minSize = size;
                if (maxSize == -1 || maxSize < size)
                    maxSize = size;
                return data;
            } catch (final Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        void destroy() {
            valid.set(false);
        }

        private void checkState() {
            if (!valid.get())
                throw new IllegalStateException("Session is invalid!");
        }
    }// END of HazelSession

    private static void addCookieForSession(final RequestWrapper req, final String sessionId) {
        final Cookie sessionCookie = new Cookie("JSESSIONID", sessionId);
        sessionCookie.setPath(req.getContextPath());
        sessionCookie.setMaxAge(-1);
        req.res.addCookie(sessionCookie);
        if (DEBUG) {
            log(req.getContextPath() + " ADDING JSESSIONID COOKIE " + sessionCookie.getValue()
                    + " now cookie.path " + sessionCookie.getPath());
        }
    }

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
        id = "HZ" + sb.toString();
        if (DEBUG) {
            log("Randomly generated session Id " + id);
        }
        return id;
    }

    private static void removeCookieForSession(final RequestWrapper req, final String sessionId) {
        final Cookie[] cookies = req.getCookies();
        if (cookies != null) {
            for (final Cookie cookie : cookies) {
                final String name = cookie.getName();
                final String value = cookie.getValue();
                final String path = cookie.getPath();
                if (req.getContextPath().equals(path)) {
                    if (name.equals("JSESSIONID") && value.equals(sessionId)) {
                        if (DEBUG) {
                            log("Found old sessionId cookie DELETING " + value);
                        }
                        cookie.setMaxAge(0);
                        req.res.addCookie(cookie);
                        break;
                    }
                }
            }
        }
    }

    public void doFilter(ServletRequest req, ServletResponse res, final FilterChain chain)
            throws IOException, ServletException {
        log("FILTERING " + req.getClass().getName());
        if (!(req instanceof HttpServletRequest)) {
            chain.doFilter(req, res);
        } else {
            if (req instanceof RequestWrapper) {
                log("Request is instance ! continue...");
                chain.doFilter(req, res);
                return;
            }
            HttpServletRequest httpReq = (HttpServletRequest) req;
            if (DEBUG) {
                final Cookie[] cookies = httpReq.getCookies();
                if (cookies != null) {
                    for (final Cookie cookie : cookies) {
                        final String name = cookie.getName();
                        final String value = cookie.getValue();
                        final String path = cookie.getPath();
                        if (name.equalsIgnoreCase("JSESSIONID")) {
                            log(path + " Request has JSESSIONID cookie " + value);
                        }
                    }
                }
            }
            boolean newRequest = (req.getAttribute(HAZELCAST_REQUEST) == null);
            final ResponseWrapper resWrapper = new ResponseWrapper((HttpServletResponse) res);
            final RequestWrapper reqWrapper = new RequestWrapper(httpReq, resWrapper);
            resWrapper.setRequest(reqWrapper);
            if (!newRequest) {
                RequestWrapper existingReq = (RequestWrapper) req.getAttribute(HAZELCAST_REQUEST);
                reqWrapper.setRequestedSessionId(existingReq.hazelSession,
                        existingReq.requestedSessionId,
                        existingReq.requestedSessionIdFromCookie);
            } else {
                reqWrapper.setExtractSessionId();
            }
            req = null;
            res = null;
            httpReq = null;
            HazelSession session = null;
            String sessionId = null;
            session = reqWrapper.getSession(false);
            if (session != null)
                sessionId = session.getId();
            if (session != null) {
                if (session.expired(System.currentTimeMillis())) {
                    if (DEBUG) {
                        log("doFilter got session expiration for " + session.getId());
                    }
                    destroySession(session);
                }
            }
            chain.doFilter(reqWrapper, resWrapper);
            if (!newRequest) return;
            req = null; // for easy debugging. reqWrapper should be used
            session = reqWrapper.getSession(false);
            if (session != null)
                sessionId = session.getId();
            if (session != null) {
                if (!session.valid.get()) {
                    if (DEBUG) {
                        log("Session is not valid. removing cookie for " + sessionId);
                    }
                    removeCookieForSession(reqWrapper, sessionId);
                    return;
                }
                final Enumeration<String> attsNames = session.getAttributeNames();
                Map mapData = null;
                while (attsNames.hasMoreElements()) {
                    final String attName = attsNames.nextElement();
                    final Object value = session.getAttribute(attName);
                    if (value instanceof Serializable) {
                        if (mapData == null) {
                            mapData = new HashMap<String, Object>();
                        }
                        mapData.put(attName, value);
                    }
                }
                boolean sessionChanged = false;
                Data data = session.writeObject(mapData);
                sessionChanged = session.sessionChanged(data);
                if (sessionChanged) {
                    if (data == null) {
                        mapData = new HashMap<String, Object>();
                        data = session.writeObject(mapData);
                    }
                    log("PUTTING SESSION " + sessionId + "  values " + mapData);
                    if (session.knownToCluster()) {
                        getClusterMap().put(sessionId, data);
                    } else {
                        Object old = getClusterMap().putIfAbsent(sessionId, data);
                        int tryCount = 1;
                        while (old != null) {
                            changeSessionId(session);
                            old = getClusterMap().putIfAbsent(sessionId, data);
                            if (tryCount++ >= 3)
                                throw new RuntimeException("SessionId Generator is no good!");
                        }
                        session.setKnownToCluster(true);
                    }
                }
                session.setLastAccessed();
                session.setNew(false);
            }
        }
    }

    public void destroy() {
        for (HazelSession session : mapSessions.values()) {
            destroySession(session);
        }
        mapSessions.clear();
    }
}// END of WebFilter

