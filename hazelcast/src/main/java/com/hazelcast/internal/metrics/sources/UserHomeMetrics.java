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

package com.hazelcast.internal.metrics.sources;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

import java.io.File;

import com.hazelcast.internal.metrics.BeforeCollectionCycle;
import com.hazelcast.internal.metrics.CollectionCycle.Tags;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.hazelcast.internal.metrics.ObjectMetricsContext;
import com.hazelcast.internal.metrics.Probe;

@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "used for metrics via reflection")
public final class UserHomeMetrics implements ObjectMetricsContext {

    private final File userHome = new File(System.getProperty("user.home"));

    @Probe(level = MANDATORY)
    private long creationTime;
    @Probe(level = MANDATORY)
    private long freeSpace;
    @Probe(level = MANDATORY)
    private long totalSpace;
    @Probe(level = MANDATORY)
    private long usableSpace;

    @Override
    public void switchToObjectContext(Tags context) {
        context.namespace("file.partition").instance("user.home");
    }

    @BeforeCollectionCycle
    private void update() {
        creationTime = userHome.lastModified();
        freeSpace = userHome.getFreeSpace();
        totalSpace = userHome.getTotalSpace();
        usableSpace = userHome.getUsableSpace();
    }

}
