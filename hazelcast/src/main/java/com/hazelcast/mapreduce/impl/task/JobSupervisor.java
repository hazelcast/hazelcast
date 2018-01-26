/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.MapReduceUtil;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.mapreduce.impl.notification.ReducingFinishedNotification;
import com.hazelcast.mapreduce.impl.operation.CancelJobSupervisorOperation;
import com.hazelcast.mapreduce.impl.operation.GetResultOperationFactory;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionProcessed;
import com.hazelcast.mapreduce.impl.operation.RequestPartitionResult;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.mapreduce.JobPartitionState.State.REDUCING;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.createJobProcessInformation;
import static com.hazelcast.mapreduce.impl.operation.RequestPartitionResult.ResultState.SUCCESSFUL;
import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static com.hazelcast.util.MapUtil.createHashMapAdapter;

/**
 * The JobSupervisor is the overall control instance of a map reduce job. There is one JobSupervisor per
 * unique name-jobId combination and per cluster member.<br/>
 * The emitting cluster member's JobSupervisor has a special control function to synchronize the work of
 * the other "worker-members" that only execute the task. This job owner node also assigns reducing members
 * to keys and checks for topology changes that hurt the currently running job and enforces the rules set
 * by the {@link com.hazelcast.mapreduce.TopologyChangedStrategy} in case of a topology change situation.
 */
public class JobSupervisor {

    private final ConcurrentMap<Object, Reducer> reducers = new ConcurrentHashMap<Object, Reducer>();
    private final ConcurrentMap<Integer, Set<Address>> remoteReducers = new ConcurrentHashMap<Integer, Set<Address>>();
    private final AtomicReference<DefaultContext> context = new AtomicReference<DefaultContext>();
    private final ConcurrentMap<Object, Address> keyAssignments = new ConcurrentHashMap<Object, Address>();

    private final Address jobOwner;
    private final boolean ownerNode;
    private final AbstractJobTracker jobTracker;
    private final JobTaskConfiguration configuration;
    private final MapReduceService mapReduceService;
    private final ExecutorService executorService;

    private final JobProcessInformationImpl jobProcessInformation;

    public JobSupervisor(JobTaskConfiguration configuration, AbstractJobTracker jobTracker, boolean ownerNode,
                         MapReduceService mapReduceService) {
        this.jobTracker = jobTracker;
        this.ownerNode = ownerNode;
        this.configuration = configuration;
        this.mapReduceService = mapReduceService;
        this.jobOwner = configuration.getJobOwner();
        this.executorService = mapReduceService.getExecutorService(configuration.getName());

        // Calculate partition count
        this.jobProcessInformation = createJobProcessInformation(configuration, this);

        // Preregister reducer task to handle immediate reducing events
        String name = configuration.getName();
        String jobId = configuration.getJobId();
        jobTracker.registerReducerTask(new ReducerTask(name, jobId, this));
    }

    public MapReduceService getMapReduceService() {
        return mapReduceService;
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    public void startTasks(MappingPhase mappingPhase) {
        // Start map-combiner tasks
        jobTracker.registerMapCombineTask(new MapCombineTask(configuration, this, mappingPhase));
    }

    public void onNotification(MapReduceNotification notification) {
        if (notification instanceof IntermediateChunkNotification) {
            IntermediateChunkNotification icn = (IntermediateChunkNotification) notification;
            ReducerTask reducerTask = jobTracker.getReducerTask(icn.getJobId());
            reducerTask.processChunk(icn.getChunk());
        } else if (notification instanceof LastChunkNotification) {
            LastChunkNotification lcn = (LastChunkNotification) notification;
            ReducerTask reducerTask = jobTracker.getReducerTask(lcn.getJobId());
            reducerTask.processChunk(lcn.getPartitionId(), lcn.getSender(), lcn.getChunk());
        } else if (notification instanceof ReducingFinishedNotification) {
            final ReducingFinishedNotification rfn = (ReducingFinishedNotification) notification;
            // Just offload it to free the event queue
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    processReducerFinished0(rfn);
                }
            });
        }
    }

    public void notifyRemoteException(Address remoteAddress, Throwable throwable) {
        // Cancel all partition states
        jobProcessInformation.cancelPartitionState();

        // Notify all other nodes about cancellation
        Set<Address> addresses = collectRemoteAddresses();

        // Cancel local job
        TrackableJobFuture future = cancel();

        // Now notify all involved members to cancel the job
        asyncCancelRemoteOperations(addresses);

        if (future != null) {
            // Might be already cancelled by another members exception
            fixAsyncStackTrace(throwable, Thread.currentThread().getStackTrace(),
                    "Operation failed on node: " + remoteAddress);
            future.setResult(throwable);
        }
    }

    public boolean cancelAndNotify(Exception exception) {
        // Cancel all partition states
        jobProcessInformation.cancelPartitionState();

        // Notify all other nodes about cancellation
        Set<Address> addresses = collectRemoteAddresses();

        // Cancel local job
        TrackableJobFuture future = cancel();

        // Now notify all involved members to cancel the job
        asyncCancelRemoteOperations(addresses);

        if (future != null) {
            // Might be already cancelled by another members exception
            future.setResult(exception);
        }

        return true;
    }

    // TODO Not yet fully supported
    /*
    public boolean cancelNotifyAndRestart() {
        // Cancel all partition states
        jobProcessInformation.cancelPartitionState();

        // Notify all other nodes about cancellation
        Set<Address> addresses = collectRemoteAddresses();

        // Now notify all involved members to cancel the job
        asyncCancelRemoteOperations(addresses);

        // Kill local tasks
        String jobId = getConfiguration().getJobId();
        MapCombineTask mapCombineTask = jobTracker.unregisterMapCombineTask(jobId);
        if (mapCombineTask != null) {
            mapCombineTask.cancel();
        }
        ReducerTask reducerTask = jobTracker.unregisterReducerTask(jobId);
        if (reducerTask != null) {
            reducerTask.cancel();
        }

        // Reset local data
        jobProcessInformation.resetPartitionState();
        reducers.clear();
        remoteReducers.clear();
        context.set(null);
        keyAssignments.clear();

        // Restart
        // TODO restart with a new KeyValueJob

        return true;
    }
    */

    public TrackableJobFuture cancel() {
        String jobId = getConfiguration().getJobId();
        TrackableJobFuture future = jobTracker.unregisterTrackableJob(jobId);
        MapCombineTask mapCombineTask = jobTracker.unregisterMapCombineTask(jobId);
        if (mapCombineTask != null) {
            mapCombineTask.cancel();
        }
        ReducerTask reducerTask = jobTracker.unregisterReducerTask(jobId);
        if (reducerTask != null) {
            reducerTask.cancel();
        }
        mapReduceService.destroyJobSupervisor(this);
        return future;
    }

    public Map<Object, Object> getJobResults() {
        DefaultContext currentContext = context.get();

        Map<Object, Object> result;
        if (configuration.getReducerFactory() != null) {
            int mapSize = MapReduceUtil.mapSize(reducers.size());
            result = createHashMapAdapter(mapSize);
            for (Map.Entry<Object, Reducer> entry : reducers.entrySet()) {
                Object reducedResults = entry.getValue().finalizeReduce();
                if (reducedResults != null) {
                    result.put(entry.getKey(), reducedResults);
                }
            }
        } else {
            // Request a possible last chunk of data
            result = currentContext.requestChunk();
        }

        // Finalize local combiners
        currentContext.finalizeCombiners();

        return result;
    }

    public <KeyIn, ValueIn, ValueOut> Reducer<ValueIn, ValueOut> getReducerByKey(Object key) {
        Reducer reducer = reducers.get(key);
        if (reducer == null && configuration.getReducerFactory() != null) {
            reducer = configuration.getReducerFactory().newReducer(key);
            Reducer oldReducer = reducers.putIfAbsent(key, reducer);
            if (oldReducer != null) {
                reducer = oldReducer;
            } else {
                reducer.beginReduce();
            }
        }
        return reducer;
    }

    public Address getReducerAddressByKey(Object key) {
        Address address = keyAssignments.get(key);
        if (address != null) {
            return address;
        }
        return null;
    }

    public Address assignKeyReducerAddress(Object key) {
        // Assign new key to a known member
        Address address = keyAssignments.get(key);
        if (address == null) {
            address = mapReduceService.getKeyMember(key);
            Address oldAddress = keyAssignments.putIfAbsent(key, address);
            if (oldAddress != null) {
                address = oldAddress;
            }
        }
        return address;
    }

    public boolean checkAssignedMembersAvailable() {
        return mapReduceService.checkAssignedMembersAvailable(keyAssignments.values());
    }

    public boolean assignKeyReducerAddress(Object key, Address address) {
        Address oldAssignment = keyAssignments.putIfAbsent(key, address);
        return oldAssignment == null || oldAssignment.equals(address);
    }

    public void checkFullyProcessed(JobProcessInformation processInformation) {
        if (isOwnerNode()) {
            JobPartitionState[] partitionStates = processInformation.getPartitionStates();
            for (JobPartitionState partitionState : partitionStates) {
                if (partitionState == null || partitionState.getState() != JobPartitionState.State.PROCESSED) {
                    return;
                }
            }

            final String name = configuration.getName();
            final String jobId = configuration.getJobId();
            final NodeEngine nodeEngine = configuration.getNodeEngine();
            final GetResultOperationFactory operationFactory = new GetResultOperationFactory(name, jobId);

            // Get the initial future object to eventually set the result and cleanup
            final TrackableJobFuture future = jobTracker.unregisterTrackableJob(jobId);
            if (future == null) {
                // If already handled just return
                return;
            }

            final JobSupervisor jobSupervisor = this;
            Runnable runnable = new GetResultsRunnable(nodeEngine, operationFactory, jobId, jobSupervisor, future);
            ExecutionService executionService = nodeEngine.getExecutionService();
            ManagedExecutorService executor = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR);
            executor.submit(runnable);
        }
    }

    public <K, V> DefaultContext<K, V> getOrCreateContext(MapCombineTask mapCombineTask) {
        DefaultContext<K, V> newContext = new DefaultContext<K, V>(configuration.getCombinerFactory(), mapCombineTask);

        if (context.compareAndSet(null, newContext)) {
            return newContext;
        }
        return context.get();
    }

    public void registerReducerEventInterests(int partitionId, Set<Address> remoteReducers) {
        Set<Address> addresses = this.remoteReducers.get(partitionId);
        if (addresses == null) {
            addresses = new CopyOnWriteArraySet<Address>();
            Set<Address> oldSet = this.remoteReducers.putIfAbsent(partitionId, addresses);
            if (oldSet != null) {
                addresses = oldSet;
            }
        }
        addresses.addAll(remoteReducers);
    }

    public Collection<Address> getReducerEventInterests(int partitionId) {
        return this.remoteReducers.get(partitionId);
    }

    public JobProcessInformationImpl getJobProcessInformation() {
        return jobProcessInformation;
    }

    public Address getJobOwner() {
        return jobOwner;
    }

    public boolean isOwnerNode() {
        return ownerNode;
    }

    public JobTaskConfiguration getConfiguration() {
        return configuration;
    }

    private void collectResults(boolean reducedResult, Map<Object, Object> mergedResults, Map.Entry entry) {
        if (reducedResult) {
            mergedResults.put(entry.getKey(), entry.getValue());

        } else {
            List<Object> list = (List) mergedResults.get(entry.getKey());
            if (list == null) {
                list = new ArrayList<Object>();
                mergedResults.put(entry.getKey(), list);
            }
            for (Object value : (List) entry.getValue()) {
                list.add(value);
            }
        }
    }

    private Set<Address> collectRemoteAddresses() {
        Set<Address> addresses = new HashSet<Address>();
        for (Set<Address> remoteReducerAddresses : remoteReducers.values()) {
            addAllFilterJobOwner(addresses, remoteReducerAddresses);
        }
        for (JobPartitionState partitionState : jobProcessInformation.getPartitionStates()) {
            if (partitionState != null && partitionState.getOwner() != null) {
                if (!partitionState.getOwner().equals(jobOwner)) {
                    addresses.add(partitionState.getOwner());
                }
            }
        }
        return addresses;
    }

    private void asyncCancelRemoteOperations(final Set<Address> addresses) {
        final NodeEngine nodeEngine = mapReduceService.getNodeEngine();
        TaskScheduler taskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        taskScheduler.execute(new Runnable() {

            @Override
            public void run() {
                String name = getConfiguration().getName();
                String jobId = getConfiguration().getJobId();
                for (Address address : addresses) {
                    try {
                        CancelJobSupervisorOperation operation = new CancelJobSupervisorOperation(name, jobId);
                        mapReduceService.processRequest(address, operation);
                    } catch (Exception ignore) {
                        // We can ignore this exception since we just want to cancel the job
                        // and the member may be crashed or unreachable in some way
                        ILogger logger = nodeEngine.getLogger(JobSupervisor.class);
                        logger.finest("Remote node may already be down", ignore);
                    }
                }
            }
        });
    }

    private void addAllFilterJobOwner(Set<Address> target, Set<Address> source) {
        for (Address address : source) {
            if (jobOwner.equals(address)) {
                continue;
            }
            target.add(address);
        }
    }

    private void processReducerFinished0(ReducingFinishedNotification notification) {
        String name = configuration.getName();
        String jobId = configuration.getJobId();
        int partitionId = notification.getPartitionId();
        Address reducerAddress = notification.getAddress();

        if (checkPartitionReductionCompleted(partitionId, reducerAddress)) {
            try {
                RequestPartitionResult result = mapReduceService
                        .processRequest(jobOwner, new RequestPartitionProcessed(name, jobId, partitionId, REDUCING));

                if (result.getResultState() != SUCCESSFUL) {
                    throw new RuntimeException("Could not finalize processing for partitionId " + partitionId);
                }
            } catch (Throwable t) {
                MapReduceUtil.notifyRemoteException(this, t);
                if (t instanceof Error) {
                    ExceptionUtil.sneakyThrow(t);
                }
            }
        }
    }

    private boolean checkPartitionReductionCompleted(int partitionId, Address reducerAddress) {
        Set<Address> remoteAddresses = remoteReducers.get(partitionId);
        if (remoteAddresses == null) {
            throw new RuntimeException("Reducer for partition " + partitionId + " not registered");
        }

        remoteAddresses.remove(reducerAddress);
        if (remoteAddresses.size() == 0) {
            if (remoteReducers.remove(partitionId) != null) {
                return true;
            }
        }
        return false;
    }

    private class GetResultsRunnable
            implements Runnable {
        private final NodeEngine nodeEngine;
        private final GetResultOperationFactory operationFactory;
        private final String jobId;
        private final JobSupervisor jobSupervisor;
        private final TrackableJobFuture future;

        public GetResultsRunnable(NodeEngine nodeEngine, GetResultOperationFactory operationFactory, String jobId,
                                  JobSupervisor jobSupervisor, TrackableJobFuture future) {
            this.nodeEngine = nodeEngine;
            this.operationFactory = operationFactory;
            this.jobId = jobId;
            this.jobSupervisor = jobSupervisor;
            this.future = future;
        }

        public void run() {
            Object finalResult = null;
            try {
                ClusterService clusterService = nodeEngine.getClusterService();
                final Collection<Member> members = clusterService.getMembers(DATA_MEMBER_SELECTOR);
                List<Map> results = MapReduceUtil.executeOperation(members, operationFactory, mapReduceService, nodeEngine);
                boolean reducedResult = configuration.getReducerFactory() != null;

                if (results != null) {
                    Map<Object, Object> mergedResults = new HashMap<Object, Object>();
                    for (Map<?, ?> map : results) {
                        for (Map.Entry entry : map.entrySet()) {
                            collectResults(reducedResult, mergedResults, entry);
                        }
                    }

                    finalResult = mergedResults;
                }
            } catch (Exception e) {
                finalResult = e;
            } finally {
                jobTracker.unregisterMapCombineTask(jobId);
                jobTracker.unregisterReducerTask(jobId);
                mapReduceService.destroyJobSupervisor(jobSupervisor);
                future.setResult(finalResult);
            }
        }
    }
}
