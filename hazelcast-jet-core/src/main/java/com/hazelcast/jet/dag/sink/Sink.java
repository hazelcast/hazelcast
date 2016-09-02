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

package com.hazelcast.jet.dag.sink;

import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.impl.job.JobContext;

import java.io.Serializable;

/**
 * Abstract class which represents any sink
 */
public interface Sink extends Serializable {
    /**
     * Return writers for the corresponding
     *
     * @return list of the data writers
     */
    DataWriter[] getWriters(JobContext taskContext);

    /**
     * Returns <tt>true</tt>if sink is partitioned.
     *
     * @return <tt>true</tt>if sink is partitioned
     */
    boolean isPartitioned();

    /**
     * @return name of the sink
     */
    String getName();
}
