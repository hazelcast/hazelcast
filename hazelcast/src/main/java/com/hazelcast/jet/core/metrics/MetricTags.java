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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.Processor;

/**
 * Metric descriptors are formed from a comma separated list of {@code
 * tag_name=tag_value} pairs. The constants defined here are the possible
 * tag names that are used in Jet. See individual descriptions for the
 * meaning of information carried by each tag.
 *
 * @since Jet 3.2
 */
public final class MetricTags {

    /**
     * Source system or module, value is always {@code "jet"}.
     */
    public static final String MODULE = "module";

    /**
     * Unique ID of the cluster member sourcing the metric.
     */
    public static final String MEMBER = "member";

    /**
     * Network address of the cluster member sourcing the metric.
     */
    public static final String ADDRESS = "address";

    /**
     * Unique ID of the job (sourcing the metric), example value would be a
     * numerical (long) ID encoded in a human readable form, like {@code
     * "2f7f-d88a-4669-6195"}, see {@link Util#idToString(long)})} for
     * details.
     */
    public static final String JOB = "job";

    /**
     * Unique ID of a particular execution of a job (sourcing the metric),
     * example value would be a numerical (long) ID encoded in a human
     * readable form, like {@code "2f7f-d88a-4669-6195"}, see {@link
     * Util#idToString(long)} for details.
     */
    public static final String EXECUTION = "exec";

    /**
     * DAG vertex name the of the metric. Example value would be {@code
     * "mapJournalSource(myMap)"}.
     */
    public static final String VERTEX = "vertex";

    /**
     * Global index of the {@link Processor} sourcing the metric.
     */
    public static final String PROCESSOR = "proc";

    /**
     * Class name without package name of the {@link Processor} sourcing
     * the metric (only for processor-specific metrics).
     */
    public static final String PROCESSOR_TYPE = "procType";

    /**
     * Boolean flag which is true if the {@link Processor} sourcing the
     * metric is a DAG source. Value is {@code true} or {@code false}.
     */
    public static final String SOURCE = "source";

    /**
     * Boolean flag which is true if the {@link Processor} sourcing the
     * metric is a DAG sink. Value is {@code true} or {@code false}.
     */
    public static final String SINK = "sink";

    /**
     * Index of the cooperative worker in a fixed worker pool sourcing the
     * metric.
     */
    public static final String COOPERATIVE_WORKER = "cooperativeWorker";

    /**
     * Index of the vertex input or output edges sourcing the metric.
     */
    public static final String ORDINAL = "ordinal";

    /**
     * Unit of metric value, for details see {@link ProbeUnit}.
     */
    public static final String UNIT = "unit";

    /**
     * Destination member address for items sent to a distributed edge.
     */
    public static final String DESTINATION_ADDRESS = "destinationAddress";

    /**
     * Source member address for items received from a distributed edge.
     */
    public static final String SOURCE_ADDRESS = "sourceAddress";

    /**
     * Boolean flag which is true if the metric is user-defined (as opposed to
     * built-in).
     *
     * @since Jet 4.0
     */
    public static final String USER = "user";

    private MetricTags() {
    }
}
