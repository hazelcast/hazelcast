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

package com.hazelcast.core;

/**
 * LifecycleService allows you to shutdown, terminate and listen to {@link LifecycleEvent}'s
 * on HazelcastInstance.
 */
public interface LifecycleService {

    /**
     * whether the instance is running
     * @return true if instance is active and running
     */
    boolean isRunning();

    /**
     * gracefully shutdowns HazelcastInstance. Different from {@link #terminate()}, waits partition operations to be completed.
     */
    void shutdown();

    /**
     * terminate HazelcastInstance ungracefully. Does not wait partition operations, forces immediate shutdown.
     */
    void terminate();

    /**
     * Add listener object to listen lifecycle events.
     * @param lifecycleListener Listener object
     * @return listener id
     */
    String addLifecycleListener(LifecycleListener lifecycleListener);

    /**
     * Remove lifecycle listener
     * @param registrationId The listener id returned by {@link #addLifecycleListener(LifecycleListener)}
     * @return true if removed successfully
     */
    boolean removeLifecycleListener(String registrationId);

}
