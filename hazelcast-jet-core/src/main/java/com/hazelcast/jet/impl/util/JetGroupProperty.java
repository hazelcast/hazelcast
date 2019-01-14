/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.spi.properties.HazelcastProperty;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the names and default values for internal Hazelcast Jet properties.
 */
public final class JetGroupProperty {

    public static final HazelcastProperty JOB_SCAN_PERIOD
            = new HazelcastProperty("jet.job.scan.period", SECONDS.toMillis(5), MILLISECONDS);
    public static final HazelcastProperty JOB_RESULTS_TTL_SECONDS
            = new HazelcastProperty("jet.job.results.ttl.seconds", DAYS.toSeconds(7), SECONDS);

    private JetGroupProperty() {
    }

}
