/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.job;

import com.hazelcast.core.Member;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.config.DeploymentConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.impl.job.deployment.Chunk;
import com.hazelcast.jet.impl.job.deployment.ChunkIterator;
import com.hazelcast.jet.impl.statemachine.job.JobEvent;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.util.JetUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.JetUtil.reThrow;

@SuppressWarnings("checkstyle:methodcount")
public abstract class JobClusterService<Payload> {
    protected final String name;
    protected JobConfig jobConfig;

    public JobClusterService(String name) {
        this.name = name;
    }

    /**
     * Init job
     *
     * @param config          job configuration
     * @param jobStateMachine state machine
     */
    public void init(JobConfig config, JobStateMachine jobStateMachine) {
        jobConfig = config;
        CompletableFuture future = submit(
                JobEvent.INIT_SUCCESS,
                JobEvent.INIT_FAILURE,
                jobStateMachine,
                () -> createInitJobInvoker(config));
        await(future);
    }

    /**
     * Performs deployment operation
     *
     * @param resources       classpath resources
     * @param jobStateMachine manager to work with job state-machine
     */
    public void deploy(Set<DeploymentConfig> resources, JobStateMachine jobStateMachine) {
        run(JobEvent.DEPLOYMENT_START,
                JobEvent.DEPLOYMENT_SUCCESS,
                JobEvent.DEPLOYMENT_FAILURE,
                jobStateMachine,
                () -> executeDeployment(resources));
    }

    /**
     * Execute job
     *
     * @param jobStateMachine manager to work with job state-machine
     * @return awaiting Future
     */
    public Future execute(JobStateMachine jobStateMachine) {
        return submit(JobEvent.EXECUTION_START,
                JobEvent.EXECUTION_SUCCESS,
                JobEvent.EXECUTION_FAILURE,
                jobStateMachine, this::createExecutionInvoker);
    }

    /**
     * Interrupt job
     *
     * @param jobStateMachine manager to work with job state-machine
     * @return awaiting Future
     */
    public Future interrupt(JobStateMachine jobStateMachine) {
        return submit(
                JobEvent.INTERRUPTION_START,
                JobEvent.INTERRUPTION_SUCCESS,
                JobEvent.INTERRUPTION_FAILURE,
                jobStateMachine,
                this::createInterruptInvoker
        );
    }

    /**
     * Finalize job
     *
     * @param jobStateMachine manager to work with job state-machine
     * @return awaiting Future
     */
    public void destroy(JobStateMachine jobStateMachine) {
        run(JobEvent.FINALIZATION_START,
                JobEvent.FINALIZATION_SUCCESS,
                JobEvent.FINALIZATION_FAILURE,
                jobStateMachine,
                () -> {
                });
    }

    /**
     * Submits dag for the job
     *
     * @param dag             direct acyclic graph
     * @param jobStateMachine manager to work with job state-machine
     */
    public void submitDag(DAG dag, JobStateMachine jobStateMachine) {
        CompletableFuture future = submit(
                JobEvent.SUBMIT_START,
                JobEvent.SUBMIT_SUCCESS,
                JobEvent.SUBMIT_FAILURE,
                jobStateMachine, () -> createSubmitInvoker(dag));
        await(future);
    }

    /**
     * @return accumulators
     */
    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> getAccumulators() {
        Set<Member> members = this.getMembers();
        Map<String, Accumulator> cache = new HashMap<>();

        try {
            for (Member member : members) {
                Future future = createInvocation(member, this::createAccumulatorsInvoker);
                Map<String, Accumulator> memberResponse = readAccumulatorsResponse(future);

                for (Map.Entry<String, Accumulator> entry : memberResponse.entrySet()) {
                    String key = entry.getKey();
                    Accumulator accumulator = entry.getValue();

                    Accumulator collector = cache.get(key);
                    if (collector == null) {
                        cache.put(key, accumulator);
                    } else {
                        collector.merge(accumulator);
                    }
                }
            }
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }

        return Collections.unmodifiableMap(cache);
    }

    /**
     * @return member of JET cluster
     */
    protected abstract Set<Member> getMembers();

    /**
     * @return invoker for interrupt operation
     */
    protected abstract Payload createInterruptInvoker();

    /**
     * @return invoker for execute operation
     */
    protected abstract Payload createExecutionInvoker();

    /**
     * @return invoker to work with accumulators
     */
    protected abstract Payload createAccumulatorsInvoker();

    /**
     * @return invoker to finish deployment
     */
    protected abstract Payload createFinishDeploymentInvoker();

    /**
     * @param chunk chunk of byte-code
     * @return invoker to deploy job
     */
    protected abstract Payload createDeploymentInvoker(Chunk chunk);

    /**
     * Invoker to send JET event
     *
     * @param jobEvent JET event
     * @return invoker
     */
    protected abstract Payload createEventInvoker(JobEvent jobEvent);

    /**
     * Return invoker to init JET job
     *
     * @param config job config
     * @return invoker to init job
     */
    protected abstract Payload createInitJobInvoker(JobConfig config);

    /**
     * Creates invocation to be called on the corresponding member
     *
     * @param <T>              type of the return value
     * @param member           member where invocation should be executed
     * @param operationFactory factory for operations
     * @return Callable object for the corresponding invocation
     */
    protected abstract <T> CompletableFuture<T> createInvocation(Member member, Supplier<Payload> operationFactory);

    protected abstract Payload createSubmitInvoker(DAG dag);

    protected abstract <T> T toObject(com.hazelcast.nio.serialization.Data data);

    protected abstract Map<String, Accumulator> readAccumulatorsResponse(Future future) throws Exception;

    protected abstract JobConfig getJobConfig();

    protected int getSecondsToAwait() {
        return getJobConfig().getSecondsToAwait();
    }

    private int getDeploymentChunkSize() {
        return getJobConfig().getChunkSize();
    }

    private List<CompletableFuture> invokeInCluster(Supplier<Payload> operationFactory) {
        Set<Member> members = getMembers();
        List<CompletableFuture> futureList = new ArrayList<>(members.size());

        for (Member member : members) {
            futureList.add(createInvocation(member, operationFactory));
        }

        return futureList;
    }

    private void await(List<CompletableFuture> list) {
        List<Throwable> errors = new ArrayList<>(list.size());
        for (Future future : list) {
            try {
                future.get();
            } catch (ExecutionException e) {
                errors.add(e.getCause());
            } catch (InterruptedException e) {
                errors.add(e);
            }
        }

        if (errors.size() >= 1) {
            throw reThrow(errors.get(0));
        }
    }

    private void await(CompletableFuture future) {
        try {
            future.get();
        } catch (ExecutionException e) {
            throw reThrow(e.getCause());
        } catch (InterruptedException e) {
            throw reThrow(e);
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    private CompletableFuture<Void> publishEvent(final JobEvent jobEvent) {
        if (jobEvent == null || jobEvent == JobEvent.FINALIZATION_SUCCESS) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture> futureList = invokeInCluster(() -> createEventInvoker(jobEvent));
        CompletableFuture[] futures = futureList.toArray(new CompletableFuture[futureList.size()]);
        return CompletableFuture.allOf(futures);
    }

    private void executeDeployment(final Set<DeploymentConfig> resources) {
        Iterator<Chunk> iterator = new ChunkIterator(resources, getDeploymentChunkSize());
        List<CompletableFuture> futures = new ArrayList<>();
        while (iterator.hasNext()) {
            final Chunk chunk = iterator.next();
            Supplier<Payload> operationFactory = () -> createDeploymentInvoker(chunk);
            futures.addAll(invokeInCluster(operationFactory));
        }
        await(futures);
        Supplier<Payload> operationFactory = this::createFinishDeploymentInvoker;
        futures.addAll(invokeInCluster(operationFactory));
        await(futures);
    }

    private void run(JobEvent startEvent,
                     JobEvent successEvent,
                     JobEvent failureEvent,
                     JobStateMachine stateMachine,
                     Runnable runnable) {
        await(publishEventAndUpdateStateMachine(startEvent, stateMachine));
        try {
            runnable.run();
            await(publishEventAndUpdateStateMachine(successEvent, stateMachine));
        } catch (Exception e) {
            await(publishEventAndUpdateStateMachine(failureEvent, stateMachine));
            throw reThrow(e);
        }
    }

    private CompletableFuture submit(JobEvent startEvent,
                                     JobEvent successEvent,
                                     JobEvent failureEvent,
                                     JobStateMachine stateMachine,
                                     Supplier<Payload> invocationFactory) {
        await(publishEventAndUpdateStateMachine(startEvent, stateMachine));
        return submit(successEvent, failureEvent, stateMachine, invocationFactory);
    }

    private CompletableFuture submit(JobEvent successEvent,
                                     JobEvent failureEvent,
                                     JobStateMachine stateMachine,
                                     Supplier<Payload> invocationFactory) {
        List<CompletableFuture> futureList = invokeInCluster(invocationFactory);
        CompletableFuture[] futures = new CompletableFuture[futureList.size()];
        futures = futureList.toArray(futures);
        CompletableFuture<Void> future = CompletableFuture.allOf(futures);
        CompletableFuture<Void> f = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            JobEvent event = throwable == null ? successEvent : failureEvent;
            CompletableFuture<Void> eventFuture = publishEventAndUpdateStateMachine(event, stateMachine);
            eventFuture.whenComplete((r, t) -> {
                if (throwable == null && t == null) {
                    f.complete(r);
                } else {
                    if (throwable != null) {
                        f.completeExceptionally(throwable);
                    } else {
                        f.completeExceptionally(t);
                    }
                }
            });
        });
        return f;
    }

    private CompletableFuture<Void> publishEventAndUpdateStateMachine(JobEvent event, JobStateMachine stateMachine) {
        return publishEvent(event).thenRun(() -> {
            stateMachine.onEvent(event);
        });
    }
}
