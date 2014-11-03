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
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.mapreduce.impl.operation.CancelJobSupervisorOperation;
import com.hazelcast.mapreduce.impl.operation.FireNotificationOperation;
import com.hazelcast.mapreduce.impl.operation.ProcessingOperation;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.JobTaskConfiguration;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
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

/**
 * The MapReduceService class is the base point for the map reduce implementation. It is used to collect
 * and control a lot of the basic lifecycle events for {@link com.hazelcast.mapreduce.JobTracker} instances
 * and their corresponding {@link com.hazelcast.mapreduce.impl.task.JobSupervisor}s.
 */
public class MapReduceService
        implements ManagedService, RemoteService {

    /**
     * The service name to retrieve an instance of the MapReduceService
     */
    public static final String SERVICE_NAME = "hz:impl:mapReduceService";

    private static final ILogger LOGGER = Logger.getLogger(MapReduceService.class);

    private static final int DEFAULT_RETRY_SLEEP_MILLIS = 100;

    private final ConstructorFunction<String, NodeJobTracker> constructor = new ConstructorFunction<String, NodeJobTracker>() {
        @Override
        public NodeJobTracker createNew(String arg) {
            JobTrackerConfig jobTrackerConfig = config.findJobTrackerConfig(arg);
            return new NodeJobTracker(arg, jobTrackerConfig.getAsReadOnly(), nodeEngine, MapReduceService.this);
        }
    };

    private final ConcurrentMap<String, NodeJobTracker> jobTrackers;
    private final ConcurrentMap<JobSupervisorKey, JobSupervisor> jobSupervisors;

    private final InternalPartitionService partitionService;
    private final ClusterService clusterService;

    private final NodeEngineImpl nodeEngine;
    private final Config config;

    public MapReduceService(NodeEngine nodeEngine) {
        this.config = nodeEngine.getConfig();
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.clusterService = nodeEngine.getClusterService();
        this.partitionService =  nodeEngine.getPartitionService();

        this.jobTrackers = new ConcurrentHashMap<String, NodeJobTracker>();
        this.jobSupervisors = new ConcurrentHashMap<JobSupervisorKey, JobSupervisor>();
    }

    public JobTracker getJobTracker(String name) {
        return (JobTracker) createDistributedObject(name);
    }

    public JobSupervisor getJobSupervisor(String name, String jobId) {
        JobSupervisorKey key = new JobSupervisorKey(name, jobId);
        return jobSupervisors.get(key);
    }

    public boolean registerJobSupervisorCancellation(String name, String jobId, Address jobOwner) {
        NodeJobTracker jobTracker = (NodeJobTracker) createDistributedObject(name);
        if (jobTracker.registerJobSupervisorCancellation(jobId) && getLocalAddress().equals(jobOwner)) {
            for (MemberImpl member : clusterService.getMemberList()) {
                if (!member.getAddress().equals(jobOwner)) {
                    try {
                        ProcessingOperation operation = new CancelJobSupervisorOperation(name, jobId);
                        processRequest(member.getAddress(), operation);
                    } catch (Exception ignore) {
                        LOGGER.finest("Member might be already unavailable", ignore);
                    }
                }
            }
            return true;
        }
        return false;
    }

    public boolean unregisterJobSupervisorCancellation(String name, String jobId) {
        NodeJobTracker jobTracker = (NodeJobTracker) createDistributedObject(name);
        return jobTracker.unregisterJobSupervisorCancellation(jobId);
    }

    public JobSupervisor createJobSupervisor(JobTaskConfiguration configuration) {
        // Job might already be cancelled (due to async processing)
        NodeJobTracker jobTracker = (NodeJobTracker) createDistributedObject(configuration.getName());
        if (jobTracker.unregisterJobSupervisorCancellation(configuration.getJobId())) {
            return null;
        }

        JobSupervisorKey key = new JobSupervisorKey(configuration.getName(), configuration.getJobId());
        boolean ownerNode = nodeEngine.getThisAddress().equals(configuration.getJobOwner());
        JobSupervisor jobSupervisor = new JobSupervisor(configuration, jobTracker, ownerNode, this);
        JobSupervisor oldSupervisor = jobSupervisors.putIfAbsent(key, jobSupervisor);
        return oldSupervisor != null ? oldSupervisor : jobSupervisor;
    }

    public boolean destroyJobSupervisor(JobSupervisor supervisor) {
        String name = supervisor.getConfiguration().getName();
        String jobId = supervisor.getConfiguration().getJobId();
        NodeJobTracker jobTracker = (NodeJobTracker) createDistributedObject(name);
        if (jobTracker != null) {
            jobTracker.unregisterJobSupervisorCancellation(jobId);
        }

        JobSupervisorKey key = new JobSupervisorKey(supervisor);
        return jobSupervisors.remove(key) == supervisor;
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
        return ConcurrencyUtil.getOrPutSynchronized(jobTrackers, objectName, jobTrackers, constructor);
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
        Address owner;
        while ((owner = partitionService.getPartitionOwner(partitionId)) == null) {
            try {
                Thread.sleep(DEFAULT_RETRY_SLEEP_MILLIS);
            } catch (Exception ignore) {
                // Partitions might not assigned yet so we need to retry
                LOGGER.finest("Partitions not yet assigned, retry", ignore);
            }
        }
        return owner;
    }

    public boolean checkAssignedMembersAvailable(Collection<Address> assignedMembers) {
        Collection<MemberImpl> members = clusterService.getMemberList();
        List<Address> addresses = new ArrayList<Address>(members.size());
        for (MemberImpl member : members) {
            addresses.add(member.getAddress());
        }
        for (Address address : assignedMembers) {
            if (!addresses.contains(address)) {
                return false;
            }
        }
        return true;
    }

    public <R> R processRequest(Address address, ProcessingOperation processingOperation)
            throws ExecutionException, InterruptedException {

        InvocationBuilder invocation = nodeEngine.getOperationService()
                                                 .createInvocationBuilder(SERVICE_NAME, processingOperation, address);

        Future<R> future = invocation.invoke();
        return future.get();
    }

    public void sendNotification(Address address, MapReduceNotification notification) {
        try {
            String name = MapReduceUtil.buildExecutorName(notification.getName());
            ProcessingOperation operation = new FireNotificationOperation(notification);
            processRequest(address, operation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public final Address getLocalAddress() {
        return nodeEngine.getThisAddress();
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public void dispatchEvent(MapReduceNotification notification) {
        String name = notification.getName();
        String jobId = notification.getJobId();
        JobSupervisor supervisor = getJobSupervisor(name, jobId);
        if (supervisor == null) {
            throw new NullPointerException("JobSupervisor name=" + name + ", jobId=" + jobId + " not found");
        }
        supervisor.onNotification(notification);
    }

    /**
     * This key type is used for assigning {@link com.hazelcast.mapreduce.impl.task.JobSupervisor}s to their
     * corresponding job trackers by JobTracker name and the unique jobId.
     */
    private static final class JobSupervisorKey {
        private final String name;
        private final String jobId;

        private JobSupervisorKey(String name, String jobId) {
            this.name = name;
            this.jobId = jobId;
        }

        private JobSupervisorKey(JobSupervisor supervisor) {
            this.name = supervisor.getConfiguration().getName();
            this.jobId = supervisor.getConfiguration().getJobId();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JobSupervisorKey that = (JobSupervisorKey) o;

            if (!jobId.equals(that.jobId)) {
                return false;
            }
            return name.equals(that.name);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
            return result;
        }
    }

}
