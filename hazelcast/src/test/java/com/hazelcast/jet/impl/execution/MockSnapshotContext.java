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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.logging.Logger;

import static com.hazelcast.test.HazelcastTestSupport.randomString;

/**
 * Simple extension of {@link SnapshotContext} that replaces usage that would
 * have otherwise been {@link org.mockito.Mockito#mock(Class)}'ed - helps
 * avoid non-deterministic Mockito-related failures on certain JVMs like Zing
 */
public class MockSnapshotContext extends SnapshotContext {
    private String currentMapName = "";
    private long currentSnapshotId = 0L;

    public MockSnapshotContext() {
        super(Logger.getLogger(MockSnapshotContext.class), randomString(), 0L, ProcessingGuarantee.NONE);

        // initialize these values to avoid assert exceptions in some tests
        initTaskletCount(1, 1, 1);
    }

    @Override
    public String currentMapName() {
        return currentMapName;
    }

    public void setCurrentMapName(String currentMapName) {
        this.currentMapName = currentMapName;
    }

    @Override
    public long currentSnapshotId() {
        return currentSnapshotId;
    }

    public void setCurrentSnapshotId(long currentSnapshotId) {
        this.currentSnapshotId = currentSnapshotId;
    }
}
