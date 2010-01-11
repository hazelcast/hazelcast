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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.*;
import com.hazelcast.monitor.client.ClusterView;
import com.hazelcast.monitor.client.ConnectionExceptoin;
import com.hazelcast.monitor.client.InstanceType;
import com.hazelcast.monitor.client.event.*;
import com.hazelcast.monitor.client.event.InstanceEvent;
import com.hazelcast.monitor.server.event.ChangeEventGenerator;
import com.hazelcast.monitor.server.event.MembershipEventGenerator;

import javax.servlet.http.HttpSession;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SessionObject {
    final static AtomicInteger idGen = new AtomicInteger(0);
    final HttpSession session;
    final static Map<Instance.InstanceType, InstanceType> instanceTypeMatchMap = fillMatchMap();
    final BlockingQueue<ChangeEvent> queue = new LinkedBlockingQueue<ChangeEvent>();
    final List<ChangeEventGenerator> eventGenerators = new CopyOnWriteArrayList<ChangeEventGenerator>();
    final Map<Integer, HazelcastClient> mapOfHz = new ConcurrentHashMap<Integer, HazelcastClient>();

    final private Object lock = new Object();
    private TimerTask task;

    public SessionObject(HttpSession session) {
        this.session = session;
        initTimer(session);
    }

    private String getName(Instance instance) {
        if (Instance.InstanceType.MAP.equals(instance.getInstanceType())) {
            return ((IMap) instance).getName();
        } else if (Instance.InstanceType.QUEUE.equals(instance.getInstanceType())) {
            return ((IQueue) instance).getName();
        } else if (Instance.InstanceType.SET.equals(instance.getInstanceType())) {
            return ((ISet) instance).getName();
        } else if (Instance.InstanceType.LIST.equals(instance.getInstanceType())) {
            return ((IList) instance).getName();
        } else if (Instance.InstanceType.MULTIMAP.equals(instance.getInstanceType())) {
            return ((MultiMap) instance).getName();
        } else if (Instance.InstanceType.TOPIC.equals(instance.getInstanceType())) {
            return ((ITopic) instance).getName();
        } else if (Instance.InstanceType.LOCK.equals(instance.getInstanceType())) {
            return ((ILock) instance).getLockObject().toString();
        } else {
            return null;
        }
    }

    private static Map<Instance.InstanceType, InstanceType> fillMatchMap() {
        Map<Instance.InstanceType, InstanceType> map =
                new ConcurrentHashMap<Instance.InstanceType, InstanceType>();
        map.put(Instance.InstanceType.MAP, InstanceType.MAP);
        map.put(Instance.InstanceType.SET, InstanceType.SET);
        map.put(Instance.InstanceType.LIST, InstanceType.LIST);
        map.put(Instance.InstanceType.QUEUE, InstanceType.QUEUE);
        map.put(Instance.InstanceType.MULTIMAP, InstanceType.MULTIMAP);
        map.put(Instance.InstanceType.TOPIC, InstanceType.TOPIC);
        map.put(Instance.InstanceType.LOCK, InstanceType.LOCK);
        map.put(Instance.InstanceType.ID_GENERATOR, InstanceType.ID_GENERATOR);
        return map;
    }

    private void initTimer(final HttpSession session) {
        Timer timer = getTimerFromServletContext(session);

        task = new TimerTask() {
            @Override
            public void run() {
                for (ChangeEventGenerator eventGenerator : eventGenerators) {
                    ChangeEvent event = eventGenerator.generateEvent();
                    queue.offer(event);
                }
            }
        };
        timer.schedule(task, new Date(), 5000);
    }

    private Timer getTimerFromServletContext(HttpSession session) {
        Timer timer = (Timer) session.getServletContext().getAttribute("timer");
        if (timer == null) {
            synchronized (lock) {
                timer = (Timer) session.getServletContext().getAttribute("timer");
                if (timer == null) {
                    timer = new Timer();
                    session.getServletContext().setAttribute("timer", timer);
                }
            }

        }
        return timer;
    }

    public ClusterView connectAndCreateClusterView(String name, String pass, String ips) throws ConnectionExceptoin {
        final int id = idGen.getAndIncrement();
        HazelcastClient client = newHazelcastClient(name, pass, ips, id);
        mapOfHz.put(id, client);
        return createClusterView(id);
    }

    private HazelcastClient newHazelcastClient(String name, String pass, String ips, final int id) throws ConnectionExceptoin {
        HazelcastClient client;
        try {
            client = HazelcastClient.newHazelcastClient(name, pass, ips);
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw new ConnectionExceptoin(e.getMessage());
        }
        client.addInstanceListener(new InstanceListener() {
            public void instanceCreated(com.hazelcast.core.InstanceEvent event) {
                Instance instance = event.getInstance();
                String name = getName(instance);
                InstanceEvent changeEvent = new InstanceCreated(id, instanceTypeMatchMap.get(event.getInstanceType()),
                        name);
                queue.offer(changeEvent);
            }

            public void instanceDestroyed(com.hazelcast.core.InstanceEvent event) {
                Instance instance = event.getInstance();
                InstanceEvent changeEvent = new InstanceDestroyed(id, instanceTypeMatchMap.get(event.getInstanceType()),
                        getName(instance));

                queue.offer(changeEvent);
            }
        });
        eventGenerators.add(new MembershipEventGenerator(client, id));

        return client;
    }

    ClusterView createClusterView(int clusterId) {
        HazelcastInstance client = mapOfHz.get(clusterId);
        ClusterView clusterView = new ClusterView();
        clusterView.setId(clusterId);
        clusterView.setGroupName(client.getName());
        MemberEvent memberEvent = (MemberEvent) new MembershipEventGenerator(client, clusterId).generateEvent();
        clusterView.getMembers().addAll(memberEvent.getMembers());
        Collection<Instance> instances = client.getInstances();

        for (Iterator<Instance> iterator = instances.iterator(); iterator.hasNext();) {
            Instance instance = iterator.next();
            if (Instance.InstanceType.MAP.equals(instance.getInstanceType())) {
                clusterView.getMaps().add(((IMap) instance).getName());
            } else if (Instance.InstanceType.QUEUE.equals(instance.getInstanceType())) {
                clusterView.getQs().add(((IQueue) instance).getName());
            } else if (Instance.InstanceType.SET.equals(instance.getInstanceType())) {
                clusterView.getSets().add(((ISet) instance).getName());
            } else if (Instance.InstanceType.LIST.equals(instance.getInstanceType())) {
                clusterView.getLists().add(((IList) instance).getName());
            } else if (Instance.InstanceType.MULTIMAP.equals(instance.getInstanceType())) {
                clusterView.getMultiMaps().add(((MultiMap) instance).getName());
            } else if (Instance.InstanceType.TOPIC.equals(instance.getInstanceType())) {
                clusterView.getTopics().add(((ITopic) instance).getName());
            } else if (Instance.InstanceType.LOCK.equals(instance.getInstanceType())) {
                clusterView.getLocks().add(((ILock) instance).getLockObject().toString());
            }
        }
        return clusterView;
    }
}
