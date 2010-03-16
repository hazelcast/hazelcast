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

package com.hazelcast.monitor.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.NoMemberAvailableException;
import com.hazelcast.monitor.client.ClusterView;
import com.hazelcast.monitor.client.ConnectionExceptoin;
import com.hazelcast.monitor.client.HazelcastService;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.exception.ClientDisconnectedException;
import com.hazelcast.monitor.server.event.ChangeEventGenerator;
import com.hazelcast.monitor.server.event.ChangeEventGeneratorFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastServiceImpl extends RemoteServiceServlet implements HazelcastService {
    private static final long serialVersionUID = 7042401980726503097L;
    private static final Object lock = new Object();
    private static final Timer timer = new Timer();
    private static final AtomicInteger idGen = new AtomicInteger(0);
    ChangeEventGeneratorFactory changeEventGeneratorFactory = new ChangeEventGeneratorFactory();

    public ClusterView connectCluster(String name, String pass, String ips) throws ConnectionExceptoin {
        final SessionObject sessionObject = getSessionObject();
        ClusterView clusterView;
        try {
            clusterView = sessionObject.connectAndCreateClusterView(name, pass, ips, idGen.getAndIncrement());
        } catch (NoMemberAvailableException e) {
            throw new ClientDisconnectedException();
        }
        return clusterView;
    }

    public ArrayList<ClusterView> loadActiveClusterViews() {
        final SessionObject sessionObject = getSessionObject();
        for


                (int clusterId : sessionObject.mapOfHz.keySet()) {
            deRegisterEvent(ChangeEventType.MAP_STATISTICS, clusterId, null);
            sessionObject.mapOfHz.get(clusterId).shutdown();
            sessionObject.cleareEventGenerators(clusterId);
        }
        sessionObject.mapOfHz.clear();
        return new ArrayList<ClusterView>();
    }

    protected SessionObject getSessionObject() {
        HttpServletRequest request = this.getThreadLocalRequest();
        HttpSession session = request.getSession();
        SessionObject sessionObject = getSessionObject(session);
        return sessionObject;
    }

    static SessionObject getSessionObject(HttpSession session) {
        String key = "session_object";
        SessionObject sessionObject = (SessionObject) session.getAttribute(key);
        if (sessionObject == null) {
            synchronized (lock) {
                if (sessionObject == null) {
                    sessionObject = new SessionObject(session, timer);
                    session.setAttribute(key, sessionObject);
                }
            }
        }
        return sessionObject;
    }

    public ArrayList<ChangeEvent> getChange() {
        SessionObject sessionObject = getSessionObject();
        ArrayList<ChangeEvent> changes = new ArrayList<ChangeEvent>();
        sessionObject.queue.drainTo(changes);
//        System.out.println("Size of the change list is:"+changes.size()+": "+this.hashCode());
        return changes;
    }

    public ChangeEvent registerEvent(ChangeEventType eventType, int clusterId, String name) {
        SessionObject sessionObject = getSessionObject();
        HazelcastClient client = sessionObject.getHazelcastClientMap().get(clusterId);
        ChangeEventGenerator eventGenerator = changeEventGeneratorFactory.createEventGenerator(eventType, clusterId, name, client);
        System.out.println("created event generator " + eventGenerator);
        System.out.println(sessionObject.getEventGenerators().contains(eventGenerator));
        if (!sessionObject.getEventGenerators().contains(eventGenerator)) {
            sessionObject.getEventGenerators().add(eventGenerator);
        }
        ChangeEvent changeEvent;
        try {
            changeEvent = eventGenerator.generateEvent();
        } catch (NoMemberAvailableException e) {
            return handleNoMemberAvailableException(clusterId, sessionObject);
        }
        return changeEvent;
    }

    private ChangeEvent handleNoMemberAvailableException(int clusterId, SessionObject sessionObject) {
        sessionObject.mapOfHz.get(clusterId).shutdown();
        sessionObject.mapOfHz.remove(clusterId);
        sessionObject.cleareEventGenerators(clusterId);
        throw new ClientDisconnectedException();
    }

    public void deRegisterEvent(ChangeEventType eventType, int clusterId, String instanceName) {
        SessionObject sessionObject = getSessionObject();
        List<ChangeEventGenerator> deleted = new ArrayList<ChangeEventGenerator>();
        for (int i = 0; i < sessionObject.getEventGenerators().size(); i++) {
            ChangeEventGenerator eventGenerator = sessionObject.getEventGenerators().get(i);
            if (eventGenerator.getChangeEventType().equals(eventType) && eventGenerator.getClusterId() == clusterId) {
                deleted.add(eventGenerator);
            }
        }
        sessionObject.getEventGenerators().removeAll(deleted);
    }
}

