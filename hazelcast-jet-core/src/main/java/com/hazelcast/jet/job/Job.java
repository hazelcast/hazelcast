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
import com.hazelcast.jet.impl.statemachine.job.JobState;
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
     * @param dag Direct acyclic graph, which describes calculation flow
     */
    void submit(DAG dag);


    /**
     * Add class to the job classLoader
     *
     * @param classes classes, which will be used during calculation
     */
    void addClass(Class... classes);

    /**
     * Add class to the job classLoader
     *
     * @param url       location of the class file
     * @param className fully qualified name of the class
     */
    void addClass(URL url, String className);

    /**
     * Add JAR to the job classLoader
     *
     * @param url location of the JAR file
     */
    void addJar(URL url);

    /**
     * Add JAR to the job classLoader
     *
     * @param url  location of the JAR file
     * @param name name of the JAR file
     */
    void addJar(URL url, String name);

    /**
     * Add resource to the job classLoader
     *
     * @param url source url with classes
     */
    void addResource(URL url);

    /**
     * Add resource to the job classLoader
     *
     * @param url  source url with classes
     * @param name name of the resource
     */
    void addResource(URL url, String name) ;

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
