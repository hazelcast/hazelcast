/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.kubernetes;

import com.hazelcast.instance.impl.ClusterTopologyIntentTracker;
import com.hazelcast.internal.json.JsonObject;

import org.junit.jupiter.api.Test;

import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.DEFAULT_STATUS_REPLICA_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RuntimeContextTest {
    // don't want to make these package private given RuntimeContext is in public API
    private static final String EXCEPTION_API_MESSAGE =
        "RuntimeContext created using deprecated constructor. For support use RuntimeContext()"
            + " and RuntimeContext(RuntimeContext)";
    private static final String EXCEPTION_COPY_CTOR_MESSAGE =
        "Copying the state of a RuntimeContext created using a deprecated API is not permitted";

    // this is the class as-is from 5.5.z as of 8199d5ce7abb0999bc435a57d9aa9fb39dbe19d3
    private static final class RuntimeContext55 {
        public static final long UNKNOWN = -1;

        private final int specifiedReplicaCount;
        private final int readyReplicas;
        private final int currentReplicas;

        @Nullable
        private final String resourceVersion;

        RuntimeContext55(int specifiedReplicaCount, int readyReplicas, int currentReplicas,
                         @Nullable String resourceVersion) {
            this.specifiedReplicaCount = specifiedReplicaCount;
            this.readyReplicas = readyReplicas;
            this.currentReplicas = currentReplicas;
            this.resourceVersion = resourceVersion;
        }

        int getSpecifiedReplicaCount() {
            return specifiedReplicaCount;
        }

        int getReadyReplicas() {
            return readyReplicas;
        }

        String getResourceVersion() {
            return resourceVersion;
        }

        int getCurrentReplicas() {
            return currentReplicas;
        }

        @Override
        public String toString() {
            return "RuntimeContext{"
                    + "specifiedReplicaCount=" + specifiedReplicaCount
                    + ", readyReplicas=" + readyReplicas
                    + ", currentReplicas=" + currentReplicas
                    + ", resourceVersion='" + resourceVersion + '\''
                    + '}';
        }
    }

    // this is the RuntimeContext class as-is from master as of 4fabf86de7f4a3719e38fb9157973c8756e2b04a
    // it's referred to RuntimeContextMaster with that context in mind
    public class RuntimeContextMaster {
        private final Map<String, StatefulSetInfo> statefulSets = new HashMap<>();
        private StatefulSetInfo merged;
        private String resourceVersion;

        public RuntimeContextMaster() { }

        public RuntimeContextMaster(RuntimeContextMaster context) {
            statefulSets.putAll(context.statefulSets);
            merged = context.merged;
            resourceVersion = context.resourceVersion;
        }

        /**
         * @param specifiedReplicaCount specified number of replicas, corresponds to {@code StatefulSetSpec.replicas}
         * @param readyReplicas         number of ready replicas, corresponds to {@code StatefulSetStatus.readyReplicas}
         * @param currentReplicas       number of replicas created by the current revision of the StatefulSet,
         *                              corresponds to {@code StatefulSetStatus.currentReplicas}
         */
        public record StatefulSetInfo(
                int specifiedReplicaCount,
                int readyReplicas,
                int currentReplicas) {

            @Nonnull
            public static StatefulSetInfo from(@Nonnull JsonObject statefulSet) {
                int specReplicas = statefulSet.get("spec").asObject().getInt("replicas", ClusterTopologyIntentTracker.UNKNOWN);
                int readyReplicas = statefulSet.get("status").asObject().getInt("readyReplicas", DEFAULT_STATUS_REPLICA_COUNT);
                int replicas = statefulSet.get("status").asObject().getInt("currentReplicas", DEFAULT_STATUS_REPLICA_COUNT);
                return new StatefulSetInfo(specReplicas, readyReplicas, replicas);
            }
        }

        private StatefulSetInfo getMerged() {
            if (merged == null) {
                merged = new StatefulSetInfo(
                        sum(StatefulSetInfo::specifiedReplicaCount),
                        sum(StatefulSetInfo::readyReplicas),
                        sum(StatefulSetInfo::currentReplicas));
            }
            return merged;
        }

        private int sum(ToIntFunction<StatefulSetInfo> fieldExtractor) {
            return statefulSets.values().stream().mapToInt(fieldExtractor)
                    .reduce((a, b) -> a == ClusterTopologyIntentTracker.UNKNOWN
                    || b == ClusterTopologyIntentTracker.UNKNOWN ? ClusterTopologyIntentTracker.UNKNOWN
                    : a + b).orElse(ClusterTopologyIntentTracker.UNKNOWN);
        }

        /**
         * Returns the total number of specified replicas. If the context
         * is empty, returns {@value ClusterTopologyIntentTracker#UNKNOWN}.
         *
         * @see StatefulSetInfo#specifiedReplicaCount()
         */
        public int getSpecifiedReplicaCount() {
            return getMerged().specifiedReplicaCount;
        }
            /**
         * Returns the total number of ready replicas. If the context
         * is empty, returns {@value ClusterTopologyIntentTracker#UNKNOWN}.
         *
         * @see StatefulSetInfo#readyReplicas()
         */
        public int getReadyReplicas() {
            return getMerged().readyReplicas;
        }

        /**
         * Returns the total number of current replicas. If the context
         * is empty, returns {@value ClusterTopologyIntentTracker#UNKNOWN}.
         *
         * @see StatefulSetInfo#currentReplicas()
         */
        public int getCurrentReplicas() {
            return getMerged().currentReplicas;
        }

        /** Returns the last resource version specified via {@link #addStatefulSetInfo}. */
        public String getResourceVersion() {
            return resourceVersion;
        }

        public int getStatefulSetCount() {
            return statefulSets.size();
        }
        public void addStatefulSetInfo(String name, @Nonnull StatefulSetInfo info, String resourceVersion) {
            StatefulSetInfo previous = statefulSets.put(name, info);
            if (!info.equals(previous)) {
                merged = null;
            }
            this.resourceVersion = resourceVersion;
        }

        @Override
        public String toString() {
            return "RuntimeContext{"
                    + "specifiedReplicaCount=" + getSpecifiedReplicaCount()
                    + ", readyReplicas=" + getReadyReplicas()
                    + ", currentReplicas=" + getCurrentReplicas()
                    + ", resourceVersion='" + resourceVersion + '\''
                    + '}';
        }
    }

    @Test
    public void testDeprecatedConstructor_SupportedOperations() {
        int specifiedReplicas = 3;
        int readyReplicas = 1;
        int currentReplicas = 2;

        var rc = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, null);
        assertEquals(specifiedReplicas, rc.getSpecifiedReplicaCount());
        assertEquals(readyReplicas, rc.getReadyReplicas());
        assertEquals(currentReplicas, rc.getCurrentReplicas());
        assertNull(rc.getResourceVersion());
    }

    // when you use the deprecated constructor you can only use the APIs that were available for
    // RuntimeContext <= 5.5
    @Test
    public void testDeprecatedConstructor_getStatefulSetCount_ThrowsUnsupportedOperationException() {
        var rc = new RuntimeContext(3, 1, 2, "version1");
        var exception = assertThrows(UnsupportedOperationException.class, rc::getStatefulSetCount);
        assertEquals(EXCEPTION_API_MESSAGE, exception.getMessage());
    }

    @Test
    public void testDeprecatedConstructor_addStatefulSetInfo_ThrowsUnsupportedOperationException() {
        var rc = new RuntimeContext(3, 1, 2, "version1");
        var info = new RuntimeContext.StatefulSetInfo(5, 3, 4);
        var exception = assertThrows(UnsupportedOperationException.class,
                () -> rc.addStatefulSetInfo("test-sts", info, "version2"));
        assertEquals(EXCEPTION_API_MESSAGE, exception.getMessage());
    }

    @Test
    public void testCopyConstructor_withLegacyContext_ThrowsUnsupportedOperationException() {
        var legacyContext = new RuntimeContext(3, 1, 2, "version1");
        var exception = assertThrows(UnsupportedOperationException.class, () -> new RuntimeContext(legacyContext));
        assertEquals(EXCEPTION_COPY_CTOR_MESSAGE, exception.getMessage());
    }

    @Test
    public void testEquivalence55_getSpecifiedReplicaCount() {
        int specifiedReplicas = 5;
        int readyReplicas = 3;
        int currentReplicas = 4;
        String resourceVersion = "test-version";
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.getSpecifiedReplicaCount(), deprecatedRuntimeContext.getSpecifiedReplicaCount());
    }

    @Test
    public void testEquivalence55_getReadyReplicas() {
        int specifiedReplicas = 5;
        int readyReplicas = 3;
        int currentReplicas = 4;
        String resourceVersion = "test-version";
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.getReadyReplicas(), deprecatedRuntimeContext.getReadyReplicas());
    }

    @Test
    public void testEquivalence55_getCurrentReplicas() {
        int specifiedReplicas = 5;
        int readyReplicas = 3;
        int currentReplicas = 4;
        String resourceVersion = "test-version";
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.getCurrentReplicas(), deprecatedRuntimeContext.getCurrentReplicas());
    }

    @Test
    public void testEquivalence55_getResourceVersion() {
        int specifiedReplicas = 5;
        int readyReplicas = 3;
        int currentReplicas = 4;
        String resourceVersion = "test-version";
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.getResourceVersion(), deprecatedRuntimeContext.getResourceVersion());
    }

    @Test
    public void testEquivalence55_getResourceVersion_withNull() {
        int specifiedReplicas = 5;
        int readyReplicas = 3;
        int currentReplicas = 4;
        String resourceVersion = null;
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.getResourceVersion(), deprecatedRuntimeContext.getResourceVersion());
        assertNull(runtimeContext55.getResourceVersion());
        assertNull(deprecatedRuntimeContext.getResourceVersion());
    }

    @Test
    public void testEquivalence55_toString() {
        int specifiedReplicas = 5;
        int readyReplicas = 3;
        int currentReplicas = 4;
        String resourceVersion = "test-version";
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.toString(), deprecatedRuntimeContext.toString());
    }

    @Test
    public void testEquivalence55_toString_withNullResourceVersion() {
        int specifiedReplicas = 7;
        int readyReplicas = 2;
        int currentReplicas = 6;
        String resourceVersion = null;
        var runtimeContext55 = new RuntimeContext55(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        var deprecatedRuntimeContext = new RuntimeContext(specifiedReplicas, readyReplicas, currentReplicas, resourceVersion);
        assertEquals(runtimeContext55.toString(), deprecatedRuntimeContext.toString());
    }

    @Test
    public void testEquivalenceMaster_emptyContext_getSpecifiedReplicaCount() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        assertEquals(runtimeContextMaster.getSpecifiedReplicaCount(), runtimeContext.getSpecifiedReplicaCount());
        assertEquals(ClusterTopologyIntentTracker.UNKNOWN, runtimeContext.getSpecifiedReplicaCount());
    }

    @Test
    public void testEquivalenceMaster_emptyContext_getReadyReplicas() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        assertEquals(runtimeContextMaster.getReadyReplicas(), runtimeContext.getReadyReplicas());
        assertEquals(ClusterTopologyIntentTracker.UNKNOWN, runtimeContext.getReadyReplicas());
    }

    @Test
    public void testEquivalenceMaster_emptyContext_getCurrentReplicas() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        assertEquals(runtimeContextMaster.getCurrentReplicas(), runtimeContext.getCurrentReplicas());
        assertEquals(ClusterTopologyIntentTracker.UNKNOWN, runtimeContext.getCurrentReplicas());
    }

    @Test
    public void testEquivalenceMaster_emptyContext_getResourceVersion() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        assertEquals(runtimeContextMaster.getResourceVersion(), runtimeContext.getResourceVersion());
        assertNull(runtimeContext.getResourceVersion());
    }

    @Test
    public void testEquivalenceMaster_emptyContext_getStatefulSetCount() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        assertEquals(runtimeContextMaster.getStatefulSetCount(), runtimeContext.getStatefulSetCount());
        assertEquals(0, runtimeContext.getStatefulSetCount());
    }

    @Test
    public void testEquivalenceMaster_singleStatefulSet() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        var info = new RuntimeContextMaster.StatefulSetInfo(5, 3, 4);
        var actualInfo = new RuntimeContext.StatefulSetInfo(5, 3, 4);
        String resourceVersion = "v1.2.3";
        runtimeContextMaster.addStatefulSetInfo("sts1", info, resourceVersion);
        runtimeContext.addStatefulSetInfo("sts1", actualInfo, resourceVersion);
        assertEquals(runtimeContextMaster.getSpecifiedReplicaCount(), runtimeContext.getSpecifiedReplicaCount());
        assertEquals(runtimeContextMaster.getReadyReplicas(), runtimeContext.getReadyReplicas());
        assertEquals(runtimeContextMaster.getCurrentReplicas(), runtimeContext.getCurrentReplicas());
        assertEquals(runtimeContextMaster.getResourceVersion(), runtimeContext.getResourceVersion());
        assertEquals(runtimeContextMaster.getStatefulSetCount(), runtimeContext.getStatefulSetCount());
        assertEquals(runtimeContextMaster.toString(), runtimeContext.toString());
    }

    @Test
    public void testEquivalenceMaster_multipleStatefulSets() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        var info1 = new RuntimeContextMaster.StatefulSetInfo(5, 3, 4);
        var actualInfo1 = new RuntimeContext.StatefulSetInfo(5, 3, 4);
        var info2 = new RuntimeContextMaster.StatefulSetInfo(7, 2, 6);
        var actualInfo2 = new RuntimeContext.StatefulSetInfo(7, 2, 6);
        String resourceVersion = "v2.0.0";
        runtimeContextMaster.addStatefulSetInfo("sts1", info1, "v1.0.0");
        runtimeContext.addStatefulSetInfo("sts1", actualInfo1, "v1.0.0");
        runtimeContextMaster.addStatefulSetInfo("sts2", info2, resourceVersion);
        runtimeContext.addStatefulSetInfo("sts2", actualInfo2, resourceVersion);
        assertEquals(runtimeContextMaster.getSpecifiedReplicaCount(), runtimeContext.getSpecifiedReplicaCount());
        assertEquals(runtimeContextMaster.getReadyReplicas(), runtimeContext.getReadyReplicas());
        assertEquals(runtimeContextMaster.getCurrentReplicas(), runtimeContext.getCurrentReplicas());
        assertEquals(runtimeContextMaster.getResourceVersion(), runtimeContext.getResourceVersion());
        assertEquals(runtimeContextMaster.getStatefulSetCount(), runtimeContext.getStatefulSetCount());
        assertEquals(runtimeContextMaster.toString(), runtimeContext.toString());
    }

    @Test
    public void testEquivalenceMaster_withUnknownValues() {
        var runtimeContextMaster = new RuntimeContextMaster();
        var runtimeContext = new RuntimeContext();
        var info1 = new RuntimeContextMaster.StatefulSetInfo(5, ClusterTopologyIntentTracker.UNKNOWN, 4);
        var actualInfo1 = new RuntimeContext.StatefulSetInfo(5, ClusterTopologyIntentTracker.UNKNOWN, 4);
        var info2 = new RuntimeContextMaster.StatefulSetInfo(7, 2, ClusterTopologyIntentTracker.UNKNOWN);
        var actualInfo2 = new RuntimeContext.StatefulSetInfo(7, 2, ClusterTopologyIntentTracker.UNKNOWN);
        String resourceVersion = "v3.0.0";
        runtimeContextMaster.addStatefulSetInfo("sts1", info1, "v1.0.0");
        runtimeContext.addStatefulSetInfo("sts1", actualInfo1, "v1.0.0");
        runtimeContextMaster.addStatefulSetInfo("sts2", info2, resourceVersion);
        runtimeContext.addStatefulSetInfo("sts2", actualInfo2, resourceVersion);
        assertEquals(runtimeContextMaster.getSpecifiedReplicaCount(), runtimeContext.getSpecifiedReplicaCount());
        assertEquals(runtimeContextMaster.getReadyReplicas(), runtimeContext.getReadyReplicas());
        assertEquals(runtimeContextMaster.getCurrentReplicas(), runtimeContext.getCurrentReplicas());
        assertEquals(runtimeContextMaster.getResourceVersion(), runtimeContext.getResourceVersion());
        assertEquals(runtimeContextMaster.getStatefulSetCount(), runtimeContext.getStatefulSetCount());
        // Verify that UNKNOWN values propagate correctly
        assertEquals(ClusterTopologyIntentTracker.UNKNOWN, runtimeContext.getReadyReplicas());
        assertEquals(ClusterTopologyIntentTracker.UNKNOWN, runtimeContext.getCurrentReplicas());
    }

    @Test
    public void testEquivalenceMaster_copyConstructor() {
        var originalMaster = new RuntimeContextMaster();
        var original = new RuntimeContext();
        var info = new RuntimeContextMaster.StatefulSetInfo(8, 5, 7);
        var actualInfo = new RuntimeContext.StatefulSetInfo(8, 5, 7);
        String resourceVersion = "v4.0.0";
        originalMaster.addStatefulSetInfo("sts1", info, resourceVersion);
        original.addStatefulSetInfo("sts1", actualInfo, resourceVersion);
        var copiedMaster = new RuntimeContextMaster(originalMaster);
        var copied = new RuntimeContext(original);
        assertEquals(copiedMaster.getSpecifiedReplicaCount(), copied.getSpecifiedReplicaCount());
        assertEquals(copiedMaster.getReadyReplicas(), copied.getReadyReplicas());
        assertEquals(copiedMaster.getCurrentReplicas(), copied.getCurrentReplicas());
        assertEquals(copiedMaster.getResourceVersion(), copied.getResourceVersion());
        assertEquals(copiedMaster.getStatefulSetCount(), copied.getStatefulSetCount());
        assertEquals(copiedMaster.toString(), copied.toString());
    }
}
