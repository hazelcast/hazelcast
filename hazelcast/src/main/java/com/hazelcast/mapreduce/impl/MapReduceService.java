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

import com.hazelcast.config.Config;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.mapreduce.impl.operation.ProcessingOperation;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.PartitionServiceImpl;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MapReduceService
        implements ManagedService, RemoteService,
        EventPublishingService<MapReduceNotification, MapReduceService> {

    public static final String SERVICE_NAME = "hz:impl:mapReduceService";
    public static final String EVENT_TOPIC_NAME = SERVICE_NAME + ".notification";

    private final ConstructorFunction<String, JobTracker> trackerConstructor = new ConstructorFunction<String, JobTracker>() {
        @Override
        public JobTracker createNew(String arg) {
            JobTrackerConfig jobTrackerConfig = config.findJobTrackerConfig(arg);
            return new NodeJobTracker(arg, jobTrackerConfig.getAsReadOnly(),
                    nodeEngine, MapReduceService.this);
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
        this.eventService.registerListener(SERVICE_NAME, EVENT_TOPIC_NAME, this);
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
        AbstractJobTracker jobTracker = (AbstractJobTracker) createDistributedObject(configuration.getName());
        boolean ownerNode = nodeEngine.getThisAddress().equals(configuration.getJobOwner());
        JobSupervisor jobSupervisor = new JobSupervisor(configuration, jobTracker, ownerNode, this);
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

    public <R> R processRequest(Address address, ProcessingOperation processingOperation)
            throws ExecutionException, InterruptedException {
        Future<R> future = nodeEngine.getOperationService().invokeOnTarget(
                MapReduceService.SERVICE_NAME, processingOperation, address);

        return future.get();
    }

    public void sendNotification(Address address, MapReduceNotification notification) {
        Collection<EventRegistration> registrations = eventService
                .getRegistrations(SERVICE_NAME, EVENT_TOPIC_NAME);

        for (EventRegistration registration : registrations) {
            if (registration.getSubscriber().equals(address)) {
                eventService.publishEvent(SERVICE_NAME, registration, notification, hashCode());
                return;
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

    public final List<Integer> getMemberPartitions(Address address) {
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

    @Override
    public void dispatchEvent(MapReduceNotification event, MapReduceService listener) {
        String name = event.getName();
        String jobId = event.getJobId();
        JobSupervisor supervisor = getJobSupervisor(name, jobId);
        if (supervisor == null) {
            throw new NullPointerException("JobSupervisor name=" + name + ", jobId=" + jobId + " not found");
        }
        supervisor.onNotification(event);
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
