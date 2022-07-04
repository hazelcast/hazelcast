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

package com.hazelcast.jet.impl;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * Constants used for Hazelcast's {@link IdentifiedDataSerializable}
 * mechanism.
 */
public final class JetFactoryIdHelper {
    /** Name of the system property that specifies Jet's data serialization factory ID. */
    public static final String JET_DS_FACTORY = "hazelcast.serialization.ds.jet";
    /** Default ID of Jet's data serialization factory. */
    public static final int JET_DS_FACTORY_ID = -10001;

    public static final String JET_IMPL_DS_FACTORY = "hazelcast.serialization.ds.jet.impl";
    public static final int JET_IMPL_DS_FACTORY_ID = -10002;

    public static final String JET_METRICS_DS_FACTORY = "hazelcast.serialization.ds.jet.metrics";
    public static final int JET_METRICS_DS_FACTORY_ID = -10003;

    public static final String JET_CONFIG_DS_FACTORY = "hazelcast.serialization.ds.jet.config";
    public static final int JET_CONFIG_DS_FACTORY_ID = -10004;

    public static final String JET_JOB_METRICS_DS_FACTORY = "hazelcast.serialization.ds.jet.job_metrics";
    public static final int JET_JOB_METRICS_DS_FACTORY_ID = -10005;

    public static final String JET_OBSERVER_DS_FACTORY = "hazelcast.serialization.ds.jet.observer";
    public static final int JET_OBSERVER_DS_FACTORY_ID = -10006;

    public static final String JET_KINESIS_DS_FACTORY = "hazelcast.serialization.ds.jet.kinesis";
    public static final int JET_KINESIS_DS_FACTORY_ID = -10007;

    public static final String JET_AGGREGATE_DS_FACTORY = "hazelcast.serialization.ds.jet.aggregate";
    public static final int JET_AGGREGATE_DS_FACTORY_ID = -10008;

    private JetFactoryIdHelper() {
    }
}
