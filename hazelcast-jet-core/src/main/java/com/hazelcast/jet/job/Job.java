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

package com.hazelcast.jet.job;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.job.localization.LocalizationResourceType;
import com.hazelcast.jet.impl.statemachine.job.JobState;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Represents abstract job
 */
public interface Job extends DistributedObject {
    /**
     * Submit dag to the cluster
     *
     * @param dag     Direct acyclic graph, which describes calculation flow
     * @param classes Classes which will be used during calculation process
     */
    void submit(DAG dag, Class... classes);


    /**
     * Add classes to the calculation's classLoader
     *
     * @param classes classes, which will be used during calculation
     * @throws IOException if resource could not be added
     */
    void addResource(Class... classes) throws IOException;

    /**
     * Add all bytecode for url to the calculation classLoader
     *
     * @param url source url with classes
     * @throws IOException if resource could not be added
     */
    void addResource(URL url) throws IOException;

    /**
     * Add all bytecode for url to the calculation classLoader
     *
     * @param inputStream              source inputStream with bytecode
     * @param name                     name of the source
     * @param localizationResourceType type of data stored in inputStream (JAR,CLASS,DATA)
     * @throws IOException if resource could not be added
     */
    void addResource(InputStream inputStream, String name, LocalizationResourceType localizationResourceType) throws IOException;

    /**
     * Clear all submitted resources
     */
    void clearResources();

    /**
     * @return state for the job's state-machine
     */
    JobState getJobState();

    /**
     * @return Returns name for the job
     */
    String getName();

    /**
     * Execute job
     *
     * @return Future which will return execution's result
     */
    Future execute();

    /**
     * Interrupts job
     *
     * @return Future which will return interruption's result
     */
    Future interrupt();

    /**
     * Returns map with statistic counters info for the certain job
     *
     * @return map with accumulators
     */
    Map<String, Accumulator> getAccumulators();


    /**
     * @return Hazelcast instance corresponding for job
     */
    HazelcastInstance getHazelcastInstance();
}
