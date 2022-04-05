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

package com.hazelcast.scheduledexecutor;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * An interface to provide means for saving &amp; loading state for {@link Runnable} and {@link java.util.concurrent.Callable}
 * tasks scheduled with an {@link IScheduledExecutorService}. When task implements this interface, the Scheduled Executor will
 * be able to handle state of the task among the replicas in an event of Migration or Node failure.
 *
 * Example:
 * <pre>
 * public class CleanUpTask implements Runnable, StatefulTask&lt;String, Integer&gt;, HazelcastInstanceAware {
 *
 *      private transient HazelcastInstance instance;
 *
 *      private transient int recordsDeletedSoFar;
 *
 *      public CleanUpTask(HazelcastInstance instance) {
 *          this.instance = instance;
 *      }
 *
 *      public void run() {
 *          recordsDeletedSoFar += cleanUpInvalidRecordsAndReturnCount();
 *      }
 *
 *      private int cleanUpInvalidRecordsAndReturnCount() {
 *      }
 *
 *      public void save(Map&lt;String, Integer&gt; snapshot) {
 *          snapshot.put("recordsDeletedSoFar", recordsDeletedSoFar);
 *      }
 *
 *      public void load(Map&lt;String, Integer&gt; snapshot) {
 *          if (state.containsKey("recordsDeletedSoFar")) {
 *              recordsDeletedSoFar = snapshot.get("recordsDeletedSoFar");
 *          }
 *      }
 * }
 * </pre>
 *
 * @param <K> The data type of the Key in the state {@link Map}
 * @param <V> The data type of the Value in the state {@link Map}
 */
public interface StatefulTask<K, V> {

    /**
     * Task callback to capture its state on the provided map. This is invoked after each invocation of {@link Runnable#run()} or
     * {@link Callable#call()} to capture a snapshot of the state and publish to replicas. If two or more replicas of the same
     * task run at the same time and publish their states, then only the one running on the owner member will be allowed to update
     * the replicas.
     *
     * <b>Note: </b> The state of the cluster is not known or guaranteed during task's execution, thus, publication of the task's
     * state to replicas is done on best-effort basis.
     *
     * Called immediately after run() or call() of the {@link Runnable} or {@link java.util.concurrent.Callable} respectively.
     *
     * @param snapshot The {@link Map} responsible for holding a snapshot of the current state.
     */
    void save(Map<K, V> snapshot);

    /**
     * Task callback to initialize its inner state, after a replica promotion, from the given map. This is invoked once per task's
     * lifecycle in a single member, before invocation of {@link Runnable#run()} or {@link Callable#call()} to setup task's state
     * as published from the previous owner of the task in the cluster.
     *
     * <code>load</code> will not be called if the snapshot is empty.
     *
     * @param snapshot The {@link Map} responsible for providing a snapshot of the task's state.
     */
    void load(Map<K, V> snapshot);

}
