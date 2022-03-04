/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.scheduledexecutor.AutoDisposableTask;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnMemberOperation;
import com.hazelcast.scheduledexecutor.impl.operations.GetAllScheduledOnPartitionOperationFactory;
import com.hazelcast.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.scheduledexecutor.impl.operations.ShutdownOperation;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.MapUtil.HASHMAP_DEFAULT_LOAD_FACTOR;
import static com.hazelcast.internal.util.MapUtil.calculateInitialCapacity;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.SERVICE_NAME;

@SuppressWarnings({"unchecked", "checkstyle:methodcount"})
public class ScheduledExecutorServiceProxy
        extends AbstractDistributedObject<DistributedScheduledExecutorService>
        implements IScheduledExecutorService {

    private static final int SHUTDOWN_TIMEOUT = 10;

    private final FutureUtil.ExceptionHandler shutdownExceptionHandler = new FutureUtil.ExceptionHandler() {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable != null) {
                if (throwable instanceof SplitBrainProtectionException) {
                    sneakyThrow(throwable);
                }
                if (throwable.getCause() instanceof SplitBrainProtectionException) {
                    sneakyThrow(throwable.getCause());
                }
            }
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "Exception while ExecutorService shutdown", throwable);
            }
        }
    };

    private final String name;
    private final ILogger logger;

    ScheduledExecutorServiceProxy(String name, NodeEngine nodeEngine, DistributedScheduledExecutorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(ScheduledExecutorServiceProxy.class);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        Callable<V> callable = createScheduledRunnableAdapter(command);
        return schedule(callable, delay, unit);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> schedule(@Nonnull Callable<V> command, long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);
        boolean autoDisposable = isAutoDisposable(command);

        TaskDefinition<V> definition = new TaskDefinition<>(TaskDefinition.Type.SINGLE_RUN, name, command, delay,
                unit, autoDisposable);

        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay,
                                                       long period, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        String name = extractNameOrGenerateOne(command);
        int partitionId = getTaskOrKeyPartitionId(command, name);
        ScheduledRunnableAdapter<?> adapter = createScheduledRunnableAdapter(command);

        TaskDefinition definition =
                new TaskDefinition(TaskDefinition.Type.AT_FIXED_RATE, name, adapter, initialDelay, period, unit, false);

        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(@Nonnull Runnable command,
                                                    @Nonnull Member member,
                                                    long delay, @Nonnull TimeUnit unit) {
        checkNotNull(member, "Member is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        Map<Member, IScheduledFuture<V>> futureMap = scheduleOnMembers(command, Collections.singleton(member), delay, unit);
        return futureMap.get(member);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnMember(@Nonnull Callable<V> command,
                                                    @Nonnull Member member,
                                                    long delay, @Nonnull TimeUnit unit) {
        checkNotNull(member, "Member is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        return scheduleOnMembers(command, Collections.singleton(member), delay, unit).get(member);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnMemberAtFixedRate(@Nonnull Runnable command,
                                                               @Nonnull Member member,
                                                               long initialDelay, long period, @Nonnull TimeUnit unit) {
        checkNotNull(member, "Member is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        Map<Member, IScheduledFuture<V>> futureMap =
                scheduleOnMembersAtFixedRate(command, Collections.singleton(member), initialDelay, period, unit);
        return futureMap.get(member);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(@Nonnull Runnable command,
                                                      @Nonnull Object key,
                                                      long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        Callable<V> callable = createScheduledRunnableAdapter(command);
        return scheduleOnKeyOwner(callable, key, delay, unit);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwner(@Nonnull Callable<V> command,
                                                      @Nonnull Object key,
                                                      long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(key, "Key is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);
        boolean autoDisposable = isAutoDisposable(command);

        TaskDefinition definition = new TaskDefinition(TaskDefinition.Type.SINGLE_RUN, name, command, delay,
                unit, autoDisposable);
        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> scheduleOnKeyOwnerAtFixedRate(@Nonnull Runnable command,
                                                                 @Nonnull Object key,
                                                                 long initialDelay, long period, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(key, "Key is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        String name = extractNameOrGenerateOne(command);
        int partitionId = getKeyPartitionId(key);
        ScheduledRunnableAdapter<?> adapter = createScheduledRunnableAdapter(command);

        TaskDefinition definition =
                new TaskDefinition(TaskDefinition.Type.AT_FIXED_RATE, name, adapter, initialDelay, period, unit, false);

        return submitOnPartitionSync(name, new ScheduleTaskOperation(getName(), definition), partitionId);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(@Nonnull Runnable command,
                                                                     long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);
        return scheduleOnMembers(command, getNodeEngine().getClusterService().getMembers(), delay, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembers(@Nonnull Callable<V> command,
                                                                     long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);
        return scheduleOnMembers(command, getNodeEngine().getClusterService().getMembers(), delay, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnAllMembersAtFixedRate(@Nonnull Runnable command, long initialDelay,
                                                                                long period, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);
        return scheduleOnMembersAtFixedRate(command, getNodeEngine().getClusterService().getMembers(), initialDelay, period,
                unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(@Nonnull Runnable command,
                                                                  @Nonnull Collection<Member> members,
                                                                  long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        Callable<V> callable = createScheduledRunnableAdapter(command);
        return scheduleOnMembers(callable, members, delay, unit);
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembers(@Nonnull Callable<V> command,
                                                                  @Nonnull Collection<Member> members,
                                                                  long delay, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        String name = extractNameOrGenerateOne(command);
        Map<Member, IScheduledFuture<V>> futures = createHashMap(members.size());
        boolean autoDisposable = isAutoDisposable(command);

        for (Member member : members) {
            TaskDefinition<V> definition = new TaskDefinition<>(TaskDefinition.Type.SINGLE_RUN, name, command, delay,
                    unit, autoDisposable);

            futures.put(member,
                    submitOnMemberSync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Nonnull
    @Override
    public <V> Map<Member, IScheduledFuture<V>> scheduleOnMembersAtFixedRate(@Nonnull Runnable command,
                                                                             @Nonnull Collection<Member> members,
                                                                             long initialDelay,
                                                                             long period, @Nonnull TimeUnit unit) {
        checkNotNull(command, "Command is null");
        checkNotNull(members, "Members is null");
        checkNotNull(unit, "Unit is null");
        command = initializeManagedContext(command);

        String name = extractNameOrGenerateOne(command);
        ScheduledRunnableAdapter<?> adapter = createScheduledRunnableAdapter(command);
        Map<Member, IScheduledFuture<V>> futures = createHashMapAdapter(members.size());

        for (Member member : members) {
            TaskDefinition definition =
                    new TaskDefinition(TaskDefinition.Type.AT_FIXED_RATE, name, adapter, initialDelay, period, unit, false);

            futures.put(member, submitOnMemberSync(name, new ScheduleTaskOperation(getName(), definition), member));
        }

        return futures;
    }

    @Nonnull
    @Override
    public <V> IScheduledFuture<V> getScheduledFuture(@Nonnull ScheduledTaskHandler handler) {
        checkNotNull(handler, "Handler is null");
        ScheduledFutureProxy proxy = new ScheduledFutureProxy(handler, this);
        return initializeManagedContext(proxy);
    }

    @Nonnull
    @Override
    public <V> Map<Member, List<IScheduledFuture<V>>> getAllScheduledFutures() {
        Map<Member, List<IScheduledFuture<V>>> accumulator = new LinkedHashMap<>();

        retrieveAllPartitionOwnedScheduled(accumulator);
        retrieveAllMemberOwnedScheduled(accumulator);

        return accumulator;
    }

    @Override
    public void shutdown() {
        NodeEngine nodeEngine = getNodeEngine();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Future> calls = new LinkedList<>();

        for (Member member : members) {
            Operation op = new ShutdownOperation(name);
            calls.add(operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress()));
        }

        waitWithDeadline(calls, SHUTDOWN_TIMEOUT, TimeUnit.SECONDS, shutdownExceptionHandler);
    }

    private <V> void retrieveAllMemberOwnedScheduled(Map<Member, List<IScheduledFuture<V>>> accumulator) {
        try {
            InvokeOnMembers invokeOnMembers =
                    new InvokeOnMembers(getNodeEngine(), getServiceName(), new GetAllScheduledOnMemberOperationFactory(name),
                            getNodeEngine().getClusterService().getMembers());
            accumulateTaskHandlersAsScheduledFutures(accumulator, invokeOnMembers.invoke());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private <V> void retrieveAllPartitionOwnedScheduled(Map<Member, List<IScheduledFuture<V>>> accumulator) {
        try {
            accumulateTaskHandlersAsScheduledFutures(accumulator,
                    getNodeEngine().getOperationService().invokeOnAllPartitions(getServiceName(),
                            new GetAllScheduledOnPartitionOperationFactory(name)));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    private <V> void accumulateTaskHandlersAsScheduledFutures(Map<Member, List<IScheduledFuture<V>>> accumulator,
                                                              Map<?, ?> taskHandlersMap) {

        ClusterService clusterService = getNodeEngine().getClusterService();
        IPartitionService partitionService = getNodeEngine().getPartitionService();

        for (Map.Entry<?, ?> entry : taskHandlersMap.entrySet()) {
            Member owner;
            Object key = entry.getKey();
            if (key instanceof Number) {
                owner = clusterService.getMember(partitionService.getPartitionOwner((Integer) key));
            } else {
                owner = (Member) key;
            }

            List<ScheduledTaskHandler> handlers = (List<ScheduledTaskHandler>) entry.getValue();
            List<IScheduledFuture<V>> futures = new ArrayList<>();

            for (ScheduledTaskHandler handler : handlers) {
                IScheduledFuture future = new ScheduledFutureProxy(handler, this);
                futures.add(initializeManagedContext(future));
            }

            if (accumulator.containsKey(owner)) {
                List<IScheduledFuture<V>> memberFutures = accumulator.get(owner);
                memberFutures.addAll(futures);
            } else {
                accumulator.put(owner, futures);
            }
        }
    }

    private <T> ScheduledRunnableAdapter<T> createScheduledRunnableAdapter(Runnable command) {
        checkNotNull(command, "Command can't be null");

        return new ScheduledRunnableAdapter<>(command);
    }

    private static <K, V> Map<K, V> createHashMapAdapter(int expectedMapSize) {
        int initialCapacity = calculateInitialCapacity(expectedMapSize);
        return new HashMapAdapter<>(initialCapacity, HASHMAP_DEFAULT_LOAD_FACTOR);
    }

    private @Nonnull
    <V> IScheduledFuture<V> createFutureProxy(int partitionId, String taskName) {
        ScheduledFutureProxy proxy =
                new ScheduledFutureProxy(ScheduledTaskHandlerImpl.of(partitionId, getName(), taskName), this);
        proxy.setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        return proxy;
    }

    private @Nonnull
    <V> IScheduledFuture<V> createFutureProxy(UUID uuid, String taskName) {
        ScheduledFutureProxy proxy = new ScheduledFutureProxy(ScheduledTaskHandlerImpl.of(uuid, getName(), taskName), this);
        proxy.setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        return proxy;
    }

    private int getKeyPartitionId(Object key) {
        return getNodeEngine().getPartitionService().getPartitionId(key);
    }

    private int getTaskOrKeyPartitionId(Callable task, Object key) {
        if (task instanceof PartitionAware) {
            Object newKey = ((PartitionAware) task).getPartitionKey();
            if (newKey != null) {
                key = newKey;
            }
        }

        return getKeyPartitionId(key);
    }

    private int getTaskOrKeyPartitionId(Runnable task, Object key) {
        if (task instanceof PartitionAware) {
            Object newKey = ((PartitionAware) task).getPartitionKey();
            if (newKey != null) {
                key = newKey;
            }
        }

        return getKeyPartitionId(key);
    }

    private String extractNameOrGenerateOne(Object command) {
        String taskName = getNamedTaskName(command);
        return taskName != null ? taskName : UuidUtil.newUnsecureUuidString();
    }

    private String getNamedTaskName(Object command) {
        if (command instanceof AbstractTaskDecorator) {
            NamedTask namedTask = ((AbstractTaskDecorator<?>) command).undecorateTo(NamedTask.class);
            if (namedTask != null) {
                return namedTask.getName();
            }
        }
        if (command instanceof NamedTask) {
            return ((NamedTask) command).getName();
        }
        return null;
    }

    private @Nonnull
    <V> IScheduledFuture<V> submitOnPartitionSync(String taskName, Operation op, int partitionId) {
        op.setPartitionId(partitionId);
        invokeOnPartition(op).joinInternal();
        return createFutureProxy(partitionId, taskName);
    }

    private @Nonnull
    <V> IScheduledFuture<V> submitOnMemberSync(String taskName, Operation op, Member member) {
        UUID uuid = member.getUuid();
        Address address = member.getAddress();
        getOperationService().invokeOnTarget(getServiceName(), op, address).joinInternal();
        return createFutureProxy(uuid, taskName);
    }

    private <T> T initializeManagedContext(Object object) {
        ManagedContext context = getNodeEngine().getSerializationService().getManagedContext();
        if (object instanceof AbstractTaskDecorator) {
            ((AbstractTaskDecorator) object).initializeContext(context);
        } else {
            object = context.initialize(object);
        }
        return (T) object;
    }

    private static class GetAllScheduledOnMemberOperationFactory
            implements Supplier<Operation> {

        private final String schedulerName;

        GetAllScheduledOnMemberOperationFactory(String schedulerName) {
            this.schedulerName = schedulerName;
        }

        @Override
        public Operation get() {
            return new GetAllScheduledOnMemberOperation(schedulerName);
        }
    }

    private boolean isAutoDisposable(Object command) {
        if (command instanceof AbstractTaskDecorator) {
            return ((AbstractTaskDecorator) command).isDecoratedWith(AutoDisposableTask.class);
        }
        return command instanceof AutoDisposableTask;
    }
}
