/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.spi.annotation.Beta;

import java.util.Set;

/**
 * Jet job metadata observer used primarily for testing.
 * It is invoked exactly before {@link InitExecutionOperation} for light jobs
 * in {@link com.hazelcast.jet.impl.LightMasterContext#createContext}.
 * <p>
 * The internal state of the observer supposed to be mutable.
 * {@link JobInvocationObserver#onLightJobInvocation} implementations must not use blocking code.
 * <p>
 * Important note: right now it executes *only* for light jobs.
 *
 * @since 5.4
 */
@Beta
public interface JobInvocationObserver {

    void onLightJobInvocation(long jobId, Set<MemberInfo> members, DAG dag, JobConfig jobConfig);
}
