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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessing;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.PartitionServiceImpl;
import com.hazelcast.spi.*;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class MapReduceService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:mapReduceService";
    public static final String EVENT_TOPIC_NAME = SERVICE_NAME + ".notification";

    private final ConstructorFunction<String, JobTracker> trackerConstructor = new ConstructorFunction<String, JobTracker>() {
        @Override
        public JobTracker createNew(String arg) {
            JobTrackerConfig jobTrackerConfig = config.findJobTrackerConfig(arg);
            return new NodeJobTracker(arg, jobTrackerConfig.getAsReadOnly(), nodeEngine);
        }
    };

    private final ConcurrentMap<String, JobTracker> jobTrackers = new ConcurrentHashMap<String, JobTracker>();
    private final ConcurrentMap<JobSupervisorKey, JobSupervisor> jobSupervisors = new ConcurrentHashMap<JobSupervisorKey, JobSupervisor>();

    private final PartitionServiceImpl partitionService;
    private final InternalPartition[] partitions;

    private final HazelcastInstance hazelcastInstance;
    private final EventService eventService;
    private final NodeEngine nodeEngine;
    private final Config config;

    public MapReduceService(NodeEngine nodeEngine) {
        this.config = nodeEngine.getConfig();
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        this.hazelcastInstance = nodeEngine.getHazelcastInstance();
        this.partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        this.partitions = partitionService.getPartitions();
        this.eventService.registerListener(SERVICE_NAME, EVENT_TOPIC_NAME, new MapReduceNotificationListener());
    }

    public JobTracker getJobTracker(String name) {
        return (JobTracker) createDistributedObject(name);
    }

    public JobSupervisor getJobSupervisor(String name, String jobId) {
        JobSupervisorKey key = new JobSupervisorKey(name, jobId);
        return jobSupervisors.get(key);
    }

    public JobSupervisor createJobSupervisor(JobTaskConfiguration configuration) {
        JobSupervisorKey key = new JobSupervisorKey(configuration.getName(), configuration.getJobId());
        JobTracker jobTracker = (JobTracker) createDistributedObject(configuration.getName());
        JobSupervisor jobSupervisor = new JobSupervisor(configuration, (AbstractJobTracker) jobTracker, this);
        JobSupervisor oldSupervisor = jobSupervisors.putIfAbsent(key, jobSupervisor);
        return oldSupervisor != null ? oldSupervisor : jobSupervisor;
    }

    public ExecutorService getExecutorService(String name) {
        return nodeEngine.getExecutionService().getExecutor(MapReduceUtil.buildExecutorName(name));
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        for (JobTracker jobTracker : jobTrackers.values()) {
            jobTracker.destroy();
        }
        jobTrackers.clear();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return ConcurrencyUtil.getOrPutSynchronized(jobTrackers, objectName, jobTrackers, trackerConstructor);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        JobTracker jobTracker = jobTrackers.remove(objectName);
        if (jobTracker != null) {
            jobTracker.destroy();
        }
    }

    public Address getKeyMember(Object key) {
        int partitionId = partitionService.getPartitionId(key);
        return partitionService.getPartitionOwner(partitionId);
    }

    public <R> R processRequest(Address address, RequestPartitionProcessing requestPartitionProcessing)
            throws ExecutionException, InterruptedException {
        Future<R> future = nodeEngine.getOperationService().invokeOnTarget(
                MapReduceService.SERVICE_NAME, requestPartitionProcessing, address);

        return future.get();
    }

    public void sendNotification(Address address, MapReduceNotification notification) {
        Collection<EventRegistration> registrations = eventService
                .getRegistrations(SERVICE_NAME, EVENT_TOPIC_NAME);

        for (EventRegistration registration : registrations) {
            if (registration.getSubscriber().equals(address)) {
                eventService.publishEvent(SERVICE_NAME, registrations, notification, hashCode());
                break;
            }
        }
    }

    public void sendNotification(MapReduceNotification notification) {
        Collection<EventRegistration> registrations = eventService
                .getRegistrations(SERVICE_NAME, EVENT_TOPIC_NAME);

        eventService.publishEvent(SERVICE_NAME, registrations, notification, hashCode());
    }

    public final List<Integer> getLocalPartitions() {
        Address address = nodeEngine.getThisAddress();
        List<Integer> partitions = new ArrayList<Integer>();
        for (InternalPartition partition : this.partitions) {
            if (partition.getReplicaAddress(0).equals(address)) {
                partitions.add(partition.getPartitionId());
            }
        }
        return partitions;
    }

    public final Address getLocalAddress() {
        return nodeEngine.getThisAddress();
    }

    private class MapReduceNotificationListener implements MessageListener<MapReduceNotification> {

        @Override
        public void onMessage(Message<MapReduceNotification> message) {
            MapReduceNotification notification = message.getMessageObject();
            String name = notification.getName();
            String jobId = notification.getJobId();
            JobSupervisor supervisor = getJobSupervisor(name, jobId);
            supervisor.onNotification(notification);
        }
    }

    private static class JobSupervisorKey {
        private final String name;
        private final String jobId;

        private JobSupervisorKey(String name, String jobId) {
            this.name = name;
            this.jobId = jobId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            JobSupervisorKey that = (JobSupervisorKey) o;

            if (!jobId.equals(that.jobId)) return false;
            if (!name.equals(that.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + jobId.hashCode();
            return result;
        }
    }

}
