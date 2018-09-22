/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.probing;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;

/**
 * Implemented by "root objects" (like core services) that know about a
 * particular set of instances they want to probe.
 *
 * Probes can have the form of objects with {@link Probe} annotated fields or
 * methods or are directly provide a value for a given name using
 * {@link ProbingCycle#gather(CharSequence, long)} (and its sibling methods).
 *
 * Implementations of {@link ProbeSource}s that are registered services at the
 * {@link ServiceManager} do not need explicit registration in the
 * {@link ProbeRegistry} as all services implementing the interface are
 * registered automatically at the end of the node startup.
 */
public interface ProbeSource {

    String TAG_INSTANCE = "instance";
    String TAG_TYPE = "type";
    String TAG_TARGET = "target";

    /**
     * Called for each {@link ProbingCycle} asking this source to probe all its
     * metrics using the provided cycle instance.
     *
     * Implementations can expect a clean context and do not have to start with
     * {@link ProbingCycle#openContext()}.
     *
     * @param cycle accumulating probing data
     */
    void probeNow(ProbingCycle cycle);
}
