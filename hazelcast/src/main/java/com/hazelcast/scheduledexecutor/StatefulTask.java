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

package com.hazelcast.scheduledexecutor;

import java.util.Map;

/**
 * An interface to provide means for saving & loading state for {@link Runnable} and {@link java.util.concurrent.Callable}
 * tasks scheduled with an {@link IScheduledExecutorService}. When task implements this interface, the Scheduled Executor
 * will be able to handle state of the task among the replicas in an event of Migration or Node failure.
 *
 * Example:
 * <pre>
 * public class CleanUpTask implements Runnable, StatefulTask<String, Integer>, HazelcastInstanceAware {
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
 *      public void save(Map<String, Integer> state) {
 *          state.put("recordsDeletedSoFar", recordsDeletedSoFar);
 *      }
 *
 *      public void load(Map<String, Integer> state) {
 *          if (state.containsKey("recordsDeletedSoFar")) {
 *              recordsDeletedSoFar = state.get("recordsDeletedSoFar");
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
     * Used to store current state of the task to a Map.
     * Keys and Values need to support serialization.
     *
     * Called immediately after run() or call() of the {@link Runnable}
     * or {@link java.util.concurrent.Callable} respectively.
     *
     * @param snapshot The {@link Map} responsible for holding a snapshot of the current state.
    */
    void save(Map<K, V> snapshot);

    /**
     * Used to load current state of the task from a Map.
     * Called once, upon initial scheduling of the task.
     *
     * @param snapshot The {@link Map} responsible for providing the snapshot of state.
     */
    void load(Map<K, V> snapshot);

}
