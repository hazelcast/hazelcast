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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WebFilter implements Filter {
    public static class ContextListener implements ServletContextListener,
            ServletContextAttributeListener {

        public void attributeAdded(final ServletContextAttributeEvent arg0) {
            if (arg0.getName().equals(Context.ATTRIBUTE_NAME))
                return;
            final AppContext app = WebFilter.ensureServletContext(arg0.getServletContext());
            if (app != null) {
                app.fireAttributeAdded(arg0);
            }
        }

        public void attributeRemoved(final ServletContextAttributeEvent arg0) {
            if (arg0.getName().equals(Context.ATTRIBUTE_NAME))
                return;
            final AppContext app = WebFilter.ensureServletContext(arg0.getServletContext());
            if (app != null) {
                app.fireAttributeRemoved(arg0);
            }
        }

        public void attributeReplaced(final ServletContextAttributeEvent arg0) {
            if (arg0.getName().equals(Context.ATTRIBUTE_NAME))
                return;
            final AppContext app = WebFilter.ensureServletContext(arg0.getServletContext());
            if (app != null) {
                app.fireAttributeReplaced(arg0);
            }
        }

        public void contextDestroyed(final ServletContextEvent arg0) {
            final AppContext app = WebFilter.getAppContext(arg0.getServletContext());
            if (app != null) {
                app.fireContextDestroyed(arg0);
            }
        }

        public void contextInitialized(final ServletContextEvent arg0) {
            final AppContext app = WebFilter.ensureServletContext(arg0.getServletContext());
            if (app != null) {
                app.fireContextInitilized(arg0);
            }
        }
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

    static class RequestWrapper extends HttpServletRequestWrapper {
        class RequestDispatcherWrapper implements RequestDispatcher {
            final RequestDispatcher dispatcherOriginal;

            final HttpServletRequest reqOriginal;

            public RequestDispatcherWrapper(final RequestDispatcher dispatcherOriginal,
                                            final HttpServletRequest reqOriginal) {
                super();
                this.dispatcherOriginal = dispatcherOriginal;
                this.reqOriginal = reqOriginal;
            }

            public void forward(final ServletRequest req, final ServletResponse res)
                    throws ServletException, IOException {
                if (DEBUG) {
                    log("FORWARDING...");
                }
                dispatcherOriginal.forward(reqOriginal, res);
            }

            public void include(final ServletRequest req, final ServletResponse res)
                    throws ServletException, IOException {
                if (DEBUG) {
                    log("INCLUDING...");
                }
                dispatcherOriginal.include(reqOriginal, res);
            }
        }

        protected static Logger logger = Logger.getLogger(RequestWrapper.class.getName());

        HazelSession hazelSession = null;

        final ResponseWrapper res;

        final ConcurrentMap<String, Object> atts = new ConcurrentHashMap<String, Object>();

        final long creationTime;

        final AppContext context;

        final HttpServletRequest original;

        String requestedSessionId = null;

        boolean requestedSessionIdValid = true;

        boolean requestedSessionIdFromCookie = false;

        boolean requestedSessionIdFromURL = false;

        public RequestWrapper(final AppContext context, final HttpServletRequest req,
                              final ResponseWrapper res) {
            super(req);
            this.res = res;
            this.context = context;
            this.original = req;
            this.original.setAttribute(HAZELCAST_REQUEST, this);
            creationTime = System.nanoTime();

            final Cookie[] cookies = req.getCookies();
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
                if (DEBUG) {
                    logger.log(Level.FINEST, "contextPath : " + getContextPath());
                    logger.log(Level.FINEST, "queryString : " + getQueryString());
                    logger.log(Level.FINEST, "requestURI : " + getRequestURI());
                    logger.log(Level.FINEST, "requestURL : " + getRequestURL());
                }
                requestedSessionId = res.extractSessionId(getRequestURL().toString());
                if (DEBUG) {
                    log("Extracted sessionId from URL " + requestedSessionId);
                }
                if (requestedSessionId != null) {
                    requestedSessionIdFromURL = true;
                }
            }
        }

        @Override
        public Enumeration getAttributeNames() {
            if (atts.size() == 0)
                return new IteratorEnumeration(null);
            return new IteratorEnumeration(atts.keySet().iterator());
        }

        @Override
        public RequestDispatcher getRequestDispatcher(final String target) {
            return new RequestDispatcherWrapper(original.getRequestDispatcher(target), original);
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
                session = context.getSession(requestedSessionId, false);
            }
            if (DEBUG) {
                log(requestedSessionId + " is requestedSessionId and  getSession : " + session);
                log("Request AppContext " + context);
            }
            if (session == null) {
                if (create) {
                    session = context.createNewSession();
                    hazelSession = session;
                    if (requestedSessionId != null) {
                        final Map mapSession = (Map) context.getClusterMap().remove(
                                requestedSessionId);
                        if (DEBUG) {
                            log(session + " Reloading from map.. " + mapSession);
                            log("ContextPath " + getContextPath());
                            log("pathInfo " + getPathInfo());
                            log("pathtranslated " + getPathTranslated());
                            log("requesturi " + getRequestURI());

                        }
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

        @Override
        public Object getAttribute(String name) {
            if (DEBUG) {
                logger.log(Level.FINEST, "getAttribute " + name);
            }
            return atts.get(name);
        }

        @Override
        public void removeAttribute(final String name) {
            if (DEBUG) {
                logger.log(Level.FINEST, "removeAttribute " + name);
            }
            if (HAZELCAST_REQUEST.equals(name))
                return;
            final Object oldValue = atts.remove(name);
            if (oldValue == null)
                return;
            if (context.lsRequestAttListeners.size() > 0) {
                executor.execute(new Runnable() {
                    public void run() {
                        final ServletRequestAttributeEvent event = new ServletRequestAttributeEvent(
                                context.getOriginalServletContext(), RequestWrapper.this, name,
                                oldValue);
                        for (final ServletRequestAttributeListener listener : context.lsRequestAttListeners) {
                            listener.attributeRemoved(event);
                        }
                    }
                });
            }
        }

        @Override
        public void setAttribute(final String name, final Object value) {
            if (DEBUG) {
                logger.log(Level.FINEST, "setAttribute " + name + " value is " + value);
            }
            if (HAZELCAST_REQUEST.equals(name))
                return;
            if (value == null) {
                removeAttribute(name);
            } else {
                final Object oldValue = atts.put(name, value);
                if (context.lsRequestAttListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            final Object eventValue = (oldValue == null) ? value : oldValue;
                            final ServletRequestAttributeEvent event = new ServletRequestAttributeEvent(
                                    context.getOriginalServletContext(), RequestWrapper.this, name,
                                    eventValue);
                            for (final ServletRequestAttributeListener listener : context.lsRequestAttListeners) {
                                if (oldValue == null)
                                    listener.attributeAdded(event);
                                else
                                    listener.attributeReplaced(event);
                            }
                        }
                    });
                }
            }
        }

        public void setRequestedSessionIdValid(final boolean valid) {
            requestedSessionIdValid = valid;
        }
    } // END of RequestWrapper

    static class ResponseWrapper extends HttpServletResponseWrapper {

        AppContext context = null;

        RequestWrapper req = null;

        public ResponseWrapper(final AppContext context, final HttpServletResponse original) {
            super(original);
            this.context = context;
        }

        @Override
        public String encodeURL(final String url) {
            if(url == null){
                throw new NullPointerException("URL can not be null");
            }
            if (!context.urlRewriteEnabled()) {
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
            if(url==null){
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

    private static class AppContext implements Context {

        protected static Logger logger = Logger.getLogger(AppContext.class.getName());

        List<ServletContextAttributeListener> lsContextAttListeners = new ArrayList<ServletContextAttributeListener>();

        List<HttpSessionListener> lsSessionListeners = new ArrayList<HttpSessionListener>();

        List<HttpSessionAttributeListener> lsSessionAttListeners = new ArrayList<HttpSessionAttributeListener>();

        List<ServletContextListener> lsContextListeners = new ArrayList<ServletContextListener>();

        List<ServletRequestListener> lsRequestListeners = new ArrayList<ServletRequestListener>();

        List<ServletRequestAttributeListener> lsRequestAttListeners = new ArrayList<ServletRequestAttributeListener>();

        List<SnapshotListener> lsSnapshotListeners = new ArrayList<SnapshotListener>();

        private final ConcurrentMap<String, HazelSession> mapSessions = new ConcurrentHashMap<String, HazelSession>(
                10);

        private final AtomicReference<Snapshot> snapshot = new AtomicReference<Snapshot>();

        private int maxInactiveInterval = 1;

        private int snapshotLifeTime;

        private final ServletContext servletContext;

        private final AtomicBoolean ready = new AtomicBoolean(false);

        private final String clusterMapName;

        private final Queue<Runnable> scheduledContextEvents = new ConcurrentLinkedQueue<Runnable>();

        public AppContext(final ServletContext servletContext) {
            this.servletContext = servletContext;
            clusterMapName = "_web_"
                    + ((appsSharingSessions) ? "shared" : servletContext.getServletContextName());
            logger.log(Level.FINEST, "CLUSTER MAP NAME " + clusterMapName);
            this.servletContext.setAttribute(Context.ATTRIBUTE_NAME, this);
            init(1);
            snapshot.set(new Snapshot(this, snapshotLifeTime));
        }

        public void addSnapshotListener(final SnapshotListener snapshotListener) {
            synchronized (lsSnapshotListeners) {
                if (DEBUG) {
                    log("CONTEXT registering a snapshot listerner " + snapshotListener);
                }
                lsSnapshotListeners.add(snapshotListener);
            }

        }

        public void destroy() {
            mapSessions.clear();
        }

        public void fireAttributeAdded(final ServletContextAttributeEvent arg0) {
            if (ready.get()) {
                if (lsContextAttListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            for (final ServletContextAttributeListener listener : lsContextAttListeners) {
                                listener.attributeAdded(arg0);
                            }
                        }
                    });
                }
            } else {
                scheduledContextEvents.add(new Runnable() {
                    public void run() {
                        fireAttributeAdded(arg0);
                    }
                });
            }
        }

        public void fireAttributeRemoved(final ServletContextAttributeEvent arg0) {
            if (ready.get()) {
                if (lsContextAttListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            for (final ServletContextAttributeListener listener : lsContextAttListeners) {
                                listener.attributeRemoved(arg0);
                            }
                        }
                    });
                }
            } else {
                scheduledContextEvents.add(new Runnable() {
                    public void run() {
                        fireAttributeRemoved(arg0);
                    }
                });
            }
        }

        public void fireAttributeReplaced(final ServletContextAttributeEvent arg0) {
            if (ready.get()) {
                if (lsContextAttListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            for (final ServletContextAttributeListener listener : lsContextAttListeners) {
                                listener.attributeReplaced(arg0);
                            }
                        }
                    });
                }
            } else {
                scheduledContextEvents.add(new Runnable() {
                    public void run() {
                        fireAttributeReplaced(arg0);
                    }
                });
            }
        }

        public void fireContextDestroyed(final ServletContextEvent arg0) {
            if (ready.get()) {
                if (lsContextListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            for (final ServletContextListener listener : lsContextListeners) {
                                listener.contextDestroyed(arg0);
                            }
                        }
                    });
                }
            }
        }

        public void fireContextInitilized(final ServletContextEvent arg0) {
            if (ready.get()) {
                if (lsContextListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            for (final ServletContextListener listener : lsContextListeners) {
                                listener.contextInitialized(arg0);
                            }
                        }
                    });
                }
            } else {
                scheduledContextEvents.add(new Runnable() {
                    public void run() {
                        fireContextInitilized(arg0);
                    }
                });
            }
        }

        public IMap getClusterMap() {
            return Hazelcast.getMap(clusterMapName);
        }

        public ServletContext getOriginalServletContext() {
            return servletContext;
        }

        public String getServletContextName() {
            return servletContext.getServletContextName();
        }

        public void init(final int maxInactiveInterval) {
            this.maxInactiveInterval = maxInactiveInterval;
            this.snapshotLifeTime = (maxInactiveInterval * 60 * 1000) / 30;
            if (maxInactiveInterval < 0)
                snapshotLifeTime = 60 * 1000;
            else if (maxInactiveInterval < 3)
                snapshotLifeTime = 5 * 1000;
            else if (maxInactiveInterval > 100)
                snapshotLifeTime = 120 * 1000;
        }

        public void removeSnapshotListener(final SnapshotListener snapshotListener) {
            synchronized (lsSnapshotListeners) {
                lsSnapshotListeners.remove(snapshotListener);
            }
        }

        public void setReady() {
            ready.set(true);
            if (scheduledContextEvents.size() > 0) {
                while (true) {
                    final Runnable scheduled = scheduledContextEvents.poll();
                    if (scheduled == null)
                        return;
                    scheduled.run();
                }
            }
        }

        public boolean urlRewriteEnabled() {
            return true;
        }

        void changeSessionId(final HazelSession session) {
            mapSessions.remove(session.getId());
            session.id = generateSessionId();
            while (mapSessions.containsKey(session.getId())) {
                session.id = generateSessionId();
            }
            mapSessions.put(session.getId(), session);
        }

        HazelSession createNewSession() {
            String id = generateSessionId();
            while (mapSessions.containsKey(id)) {
                id = generateSessionId();
            }
            return getSession(id, true);
        }

        void destroySession(final HazelSession session) {
            final String id = session.id;
            if (lsSessionListeners.size() > 0) {
                executor.execute(new Runnable() {
                    public void run() {
                        final HttpSessionEvent event = new HttpSessionEvent(session);
                        for (final HttpSessionListener listener : lsSessionListeners) {
                            listener.sessionDestroyed(event);
                        }
                    }
                });
            }
            session.destroy();
            mapSessions.remove(id);
            getClusterMap().remove(id);
            getSnapshot().destroyedSessions.incrementAndGet();
        }

        HazelSession getSession(final String sessionId, final boolean create) {
            HazelSession session = mapSessions.get(sessionId);
            if (create && session == null) {
                session = new HazelSession(this, sessionId);
                session.setMaxInactiveInterval(maxInactiveInterval * 60);

                final HazelSession oldSessionInfo = mapSessions.putIfAbsent(sessionId, session);
                if (oldSessionInfo != null) {
                    session = oldSessionInfo;
                }
            }
            return session;
        }

        Snapshot getSnapshot() {
            Snapshot s = snapshot.get();
            if (s.invalid()) {
                if (DEBUG) {
                    log("Snapshot is not valid");
                }
                synchronized (Snapshot.class) {
                    s = snapshot.get();
                    if (s.invalid()) {
                        final Snapshot sNew = new Snapshot(this, snapshotLifeTime);
                        final boolean ok = snapshot.compareAndSet(s, sNew);
                        if (ok) {
                            fireSnapshotEvent(s.createSnapshotEvent());
                            return sNew;
                        } else
                            return snapshot.get();
                    }
                }
            }
            return s;
        }

        private void fireSnapshotEvent(final SnapshotEvent snapshotEvent) {
            if (DEBUG) {
                log(lsSnapshotListeners.size() + " FireSnapshotEvent " + snapshotEvent);
            }

            synchronized (lsSnapshotListeners) {
                for (final SnapshotListener listener : lsSnapshotListeners) {
                    executor.execute(new Runnable() {
                        public void run() {
                            listener.handleSnapshot(snapshotEvent);
                        }
                    });
                }
            }

        }

    } // END of AppContext

    private static class Controller implements Runnable {

        public void control(final AppContext app) {
            try {
                if (DEBUG) {
                    log("Controller checking the sessions");
                }
                final Collection<HazelSession> sessions = app.mapSessions.values();
                final long currentTime = System.currentTimeMillis();
                for (final HazelSession session : sessions) {
                    if (session != null) {
                        if (session.expired(currentTime) || !session.valid.get()) {
                            final String id = session.id;
                            if (DEBUG) {
                                log("Controller removing a session " + id);
                            }
                            app.destroySession(session);
                        }
                    } else {
                        if (DEBUG) {
                            log("SessionInfo got null hazelsession " + session);
                        }
                    }
                }
            } catch (final Throwable t) {
                if (DEBUG) {
                    t.printStackTrace();
                }
            }
        }

        public void run() {
            for (AppContext appContext : mapApps.values()) {
                control(appContext);
            }
        }
    } // END of Controller

    private static class HazelSession implements HttpSession {
        private byte[] hash = null;

        private MessageDigest md = null;

        public int minSize = -1;

        public int maxSize = -1;

        AtomicLong maxInactiveInterval = new AtomicLong(30 * 60 * 1000);

        AtomicLong creationTime = new AtomicLong();

        AtomicLong lastAccessedTime = new AtomicLong();

        AtomicBoolean valid = new AtomicBoolean(true);

        AtomicBoolean isNew = new AtomicBoolean(true);

        AtomicBoolean knownToCluster = new AtomicBoolean(false);

        String id = null;

        ConcurrentMap<String, Object> atts = new ConcurrentHashMap<String, Object>();

        AppContext context = null;

        public HazelSession(final AppContext context, final String sessionId) {
            this.context = context;
            try {
                md = MessageDigest.getInstance("md5");
            } catch (final NoSuchAlgorithmException e) {
            }
            this.id = sessionId;
            creationTime.set(System.currentTimeMillis());
            lastAccessedTime.set(System.currentTimeMillis());
            final List<HttpSessionListener> lsSessionListeners = context.lsSessionListeners;
            if (DEBUG) {
                log("Creating session " + lsSessionListeners.size());
            }
            if (lsSessionListeners.size() > 0) {
                executor.execute(new Runnable() {
                    public void run() {
                        final HttpSessionEvent event = new HttpSessionEvent(HazelSession.this);
                        for (final HttpSessionListener listener : lsSessionListeners) {
                            listener.sessionCreated(event);
                        }
                    }
                });
            }
            context.getSnapshot().createdSessions.incrementAndGet();

        }

        public boolean expired(final long currentTime) {
            final long maxInactive = maxInactiveInterval.get();
            if (maxInactive < 0)
                return false;
            return (currentTime - lastAccessedTime.get()) >= maxInactive;
        }

        public Object getAttribute(final String name) {
            checkState();
            return atts.get(name);
        }

        public Enumeration getAttributeNames() {
            checkState();
            if (atts.size() == 0)
                return new IteratorEnumeration(null);
            return new IteratorEnumeration(atts.keySet().iterator());
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
            return context.getOriginalServletContext();
        }

        public HttpSessionContext getSessionContext() {
            checkState();
            return null;
        }

        public Object getValue(final String name) {
            checkState();
            return atts.get(name);
        }

        public String[] getValueNames() {
            checkState();
            return atts.keySet().toArray(new String[atts.size()]);
        }

        public byte[] hash(final Data data) {
            if (data == null)
                return null;
            md.reset();
            md.digest(data.buffer.array());
            return md.digest();
        }
 
        public void invalidate() {
            checkState();
            context.destroySession(this);
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
            setAttribute(name, value);
        }

        public void removeAttribute(final String name) {
            checkState();
            final Object oldValue = atts.remove(name);
            if (oldValue != null) {
                if (oldValue instanceof HttpSessionBindingListener) {
                    executor.execute(new Runnable() {
                        public void run() {
                            HttpSessionBindingEvent bindingEvent;
                            bindingEvent = new HttpSessionBindingEvent(HazelSession.this, name,
                                    oldValue);
                            ((HttpSessionBindingListener) oldValue).valueUnbound(bindingEvent);
                        }
                    });
                }
                final List<HttpSessionAttributeListener> lsSessionAttributeListeners = context.lsSessionAttListeners;
                if (lsSessionAttributeListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            final HttpSessionBindingEvent event = new HttpSessionBindingEvent(
                                    HazelSession.this, name, oldValue);
                            for (final HttpSessionAttributeListener listener : lsSessionAttributeListeners) {
                                listener.attributeRemoved(event);
                            }
                        }
                    });
                }
            }
        }

        public void removeValue(final String name) {
            checkState();
            removeAttribute(name);
        }

        public boolean sessionChanged(final Data data) {
            if (data == null) {
                if (hash == null) {
                    return false;
                } else {
                    hash = null;
                    return true;
                }
            }
            final byte[] newHash = hash(data);
            if (hash == null) {
                hash = newHash;
                return true;
            }
            final boolean same = Arrays.equals(hash, newHash);
            if (!same) {
                hash = newHash;
                return true;
            }
            return false;
        }

        public void setAttribute(final String name, final Object value) {
            checkState();
            if (DEBUG) {
                log(name + " Setting attribute !!! " + context.lsSessionAttListeners.size());
            }
            if (value == null) {
                removeAttribute(name);
            } else {
                final Object oldValue = atts.put(name, value);
                if (value instanceof HttpSessionBindingListener) {
                    executor.execute(new Runnable() {
                        public void run() {
                            final HttpSessionBindingEvent event = new HttpSessionBindingEvent(
                                    HazelSession.this, name, value);
                            final HttpSessionBindingListener listener = (HttpSessionBindingListener) value;
                            listener.valueBound(event);
                        }
                    });
                }
                if (oldValue != null && oldValue instanceof HttpSessionBindingListener) {
                    executor.execute(new Runnable() {
                        public void run() {
                            final HttpSessionBindingEvent event = new HttpSessionBindingEvent(
                                    HazelSession.this, name, oldValue);
                            final HttpSessionBindingListener listener = (HttpSessionBindingListener) value;
                            listener.valueUnbound(event);
                        }
                    });
                }
                final List<HttpSessionAttributeListener> lsSessionAttributeListeners = context.lsSessionAttListeners;
                if (lsSessionAttributeListeners.size() > 0) {
                    executor.execute(new Runnable() {
                        public void run() {
                            Object eventValue = value;
                            if (oldValue != null)
                                eventValue = oldValue;
                            final HttpSessionBindingEvent event = new HttpSessionBindingEvent(
                                    HazelSession.this, name, eventValue);
                            for (final HttpSessionAttributeListener listener : lsSessionAttributeListeners) {
                                if (oldValue != null) {
                                    listener.attributeReplaced(event);
                                } else {
                                    listener.attributeAdded(event);
                                }
                            }
                        }
                    });
                }
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
            context = null;
            atts.clear();
            md = null;
        }

        private void checkState() {
            if (!valid.get())
                throw new IllegalStateException("Session is invalid!");
        }
    }// END of HazelSession

    private static class Snapshot {
        private final long createTime;

        private final long lifeTime;

        private final AppContext context;

        public AtomicLong aveRequestTime = new AtomicLong();

        public AtomicLong minRequestTime = new AtomicLong(Long.MAX_VALUE);

        public AtomicLong maxRequestTime = new AtomicLong(Long.MIN_VALUE);

        public AtomicLong numberOfRequests = new AtomicLong();

        public AtomicInteger createdSessions = new AtomicInteger();

        public AtomicInteger destroyedSessions = new AtomicInteger();

        private final Object averageLock = new Object();

        public AtomicLong tempNumberOfRequests = new AtomicLong();

        public AtomicLong tempTotalReqTime = new AtomicLong();

        public Snapshot(final AppContext context, final long snapshotLifeTime) {
            createTime = System.currentTimeMillis();
            this.lifeTime = snapshotLifeTime;
            this.context = context;
        }

        public SnapshotEvent createSnapshotEvent() {
            flush();
            final long minReqT = (minRequestTime.get() == Long.MAX_VALUE) ? 0 : minRequestTime
                    .get();
            final long maxReqT = (maxRequestTime.get() == Long.MIN_VALUE) ? 0 : maxRequestTime
                    .get();

            return new SnapshotEvent(context.getOriginalServletContext(), createdSessions.get(),
                    destroyedSessions.get(), minReqT, maxReqT, aveRequestTime.get(),
                    numberOfRequests.get());
        }

        public boolean invalid() {
            return (System.currentTimeMillis() - createTime) > lifeTime;
        }

        public void requestTime(final long nano) {
            if (nano < minRequestTime.get())
                minRequestTime.set(nano);
            if (nano > maxRequestTime.get())
                maxRequestTime.set(nano);
            final long tempCount = tempNumberOfRequests.incrementAndGet();
            tempTotalReqTime.addAndGet(nano);
            if (tempCount > 10000) {
                synchronized (averageLock) {
                    if (tempCount > 10000) {
                        flush();
                    }
                }
            }
        }

        void flush() {
            final long tempReqCount = tempNumberOfRequests.get();
            if (tempReqCount > 0) {
                final long temReqTime = tempTotalReqTime.get();
                final long aveReqTime = aveRequestTime.get();
                final long reqs = numberOfRequests.get();

                final long totalTime = ((aveReqTime * reqs) + temReqTime);
                final long totalReqCount = reqs + tempReqCount;

                final long newAve = totalTime / totalReqCount;

                aveRequestTime.set(newAve);
                numberOfRequests.set(totalReqCount);

                tempNumberOfRequests.set(0);
                tempTotalReqTime.set(0);
            }
        }
    }

    protected static Logger logger = Logger.getLogger(WebFilter.class.getName());

    private static final boolean DEBUG = false;

    private static final String SESSION_URL_PHRASE = ";jsessionid=";

    public static final String HAZELCAST_REQUEST = "*hazelcast-request";

    private static ConcurrentMap<String, AppContext> mapApps = new ConcurrentHashMap<String, AppContext>(
            10);

    private AppContext app = null;

    private static boolean appsSharingSessions = false;

    private static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);

    static {
        executor.scheduleAtFixedRate(new Controller(), 0, 60, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                for (AppContext appContext : mapApps.values()) {
                    appContext.getSnapshot();
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    public WebFilter() {
    }

    public static synchronized AppContext ensureServletContext(final ServletContext servletContext) {
        AppContext app = getAppContext(servletContext.getServletContextName());
        if (app == null) {
            app = new AppContext(servletContext);
            setAppContext(servletContext.getServletContextName(), app);
        }
        return app;
    }

    public static synchronized AppContext getAppContext(String servletContextName) {
        if (appsSharingSessions) {
            servletContextName = "_hz_shared_app";
        }
        return mapApps.get(servletContextName);
    }

    public static synchronized ServletContext getServletContext(final ServletContext original) {
        final AppContext app = getAppContext(original.getServletContextName());
        if (app == null)
            return original;
        return app.getOriginalServletContext();
    }

    public static synchronized AppContext setAppContext(String servletContextName,
                                                        final AppContext app) {
        if (appsSharingSessions) {
            servletContextName = "_hz_shared_app";
        }
        log(appsSharingSessions + " PUTTING.. " + servletContextName + " appobj " + app);
        return mapApps.put(servletContextName, app);
    }

    static void log(final Object obj) {
        if (DEBUG) {
            logger.log(Level.FINEST, obj.toString());
        }
    }

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

    private static AppContext getAppContext(final ServletContext servletContext) {
        return getAppContext(servletContext.getServletContextName());
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

    public void destroy() {
        mapApps.remove(app.getServletContextName());
    }

    public void doFilter(ServletRequest req, ServletResponse res, final FilterChain chain)
            throws IOException, ServletException {
        log("doFILTER");
        if (DEBUG) {
            log(appsSharingSessions + " FILTERING %%55555.. " + req.getClass().getName());

        }
        if (!(req instanceof HttpServletRequest)) {
            chain.doFilter(req, res);
        } else {
            if (req instanceof RequestWrapper) {
                chain.doFilter(req, res);
                return;
            } else {
                if (req.getAttribute(HAZELCAST_REQUEST) != null) {
                    chain.doFilter(req, res);
                    return;
                }
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
            final ResponseWrapper resWrapper = new ResponseWrapper(app, (HttpServletResponse) res);
            final RequestWrapper reqWrapper = new RequestWrapper(app, httpReq, resWrapper);
            resWrapper.setRequest(reqWrapper);

            final ServletRequestEvent event = (app.lsRequestListeners.size() == 0) ? null
                    : new ServletRequestEvent(app.getOriginalServletContext(), reqWrapper);
            if (event != null) {
                executor.execute(new Runnable() {
                    public void run() {
                        for (final ServletRequestListener listener : app.lsRequestListeners) {
                            listener.requestInitialized(event);
                        }
                    }
                });
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
                    app.destroySession(session);
                }
            }
            chain.doFilter(reqWrapper, resWrapper);
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
                    if (DEBUG) {
                        log("PUTTING SESSION " + sessionId);
                    }
                    if (session.knownToCluster()) {
                        app.getClusterMap().put(sessionId, data);
                    } else {
                        Object old = app.getClusterMap().putIfAbsent(sessionId, data);
                        int tryCount = 1;
                        while (old != null) {
                            app.changeSessionId(session);
                            old = app.getClusterMap().putIfAbsent(sessionId, data);
                            if (tryCount++ >= 3)
                                throw new RuntimeException("SessinId Generator is no good!");
                        }
                        session.setKnownToCluster(true);
                    }
                }
                session.setLastAccessed();
                session.setNew(false);
            }
            app.getSnapshot().requestTime((System.nanoTime() - reqWrapper.creationTime) / 1000);
            if (event != null) {
                executor.execute(new Runnable() {
                    public void run() {
                        for (final ServletRequestListener listener : app.lsRequestListeners) {
                            listener.requestDestroyed(event);
                        }
                    }
                });
            }
        }
    }

    public void init(final FilterConfig config) throws ServletException {

        int maxInactiveInterval = 30; // minutes

        final String appsSharingSessionsValue = config.getInitParameter("apps-sharing-sessions");
        if (appsSharingSessionsValue != null) {
            appsSharingSessions = Boolean.valueOf(appsSharingSessionsValue.trim());
        }

        final String sessionTimeoutValue = config.getInitParameter("session-timeout");
        if (sessionTimeoutValue != null) {
            maxInactiveInterval = Integer.parseInt(sessionTimeoutValue.trim());
        }
        app = ensureServletContext(config.getServletContext());
        app.init(maxInactiveInterval);
        final int listenerCount = Integer.parseInt(config.getInitParameter("listener-count"));
        for (int i = 0; i < listenerCount; i++) {
            final String listenerClass = config.getInitParameter("listener" + i);
            if (DEBUG) {
                log("Found listener " + listenerClass);
            }
            try {
                final Object listener = Class.forName(listenerClass).newInstance();
                if (listener instanceof HttpSessionListener) {
                    app.lsSessionListeners.add((HttpSessionListener) listener);
                }
                if (listener instanceof HttpSessionAttributeListener) {
                    app.lsSessionAttListeners.add((HttpSessionAttributeListener) listener);
                }

                if (listener instanceof ServletContextListener) {
                    app.lsContextListeners.add((ServletContextListener) listener);
                }

                if (listener instanceof ServletContextAttributeListener) {
                    app.lsContextAttListeners.add((ServletContextAttributeListener) listener);
                }

                if (listener instanceof ServletRequestListener) {
                    app.lsRequestListeners.add((ServletRequestListener) listener);
                }

                if (listener instanceof ServletRequestAttributeListener) {
                    app.lsRequestAttListeners.add((ServletRequestAttributeListener) listener);
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
        app.setReady();
    }

}// END of WebFilter

