/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.UNKNOWN;

@NotThreadSafe
public class RuntimeContext {

    private final Map<String, StatefulSetInfo> statefulSets = new HashMap<>();
    private StatefulSetInfo merged;
    private String resourceVersion;

    public RuntimeContext() { }

    public RuntimeContext(RuntimeContext context) {
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
            int specReplicas = statefulSet.get("spec").asObject().getInt("replicas", UNKNOWN);
            int readyReplicas = statefulSet.get("status").asObject().getInt("readyReplicas", UNKNOWN);
            int replicas = statefulSet.get("status").asObject().getInt("currentReplicas", UNKNOWN);
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
                .reduce((a, b) -> a == UNKNOWN || b == UNKNOWN ? UNKNOWN : a + b).orElse(UNKNOWN);
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
