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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.DEFAULT_STATUS_REPLICA_COUNT;

/**
 * Models the state of a Kubernetes StatefulSet(s).
 * Note that this class is not thread-safe when instantiated using {@link #RuntimeContext()} or
 * {@link #RuntimeContext(RuntimeContext)}.
 * {@link #RuntimeContext(int, int, int, String)} models a single StatefulSet.
 */
@NotThreadSafe
public class RuntimeContext {
    /**
     * Unknown value
     */
    @Deprecated(forRemoval = true, since = "5.6")
    public static final long UNKNOWN = -1;
    private static final String UNSUPPORTED_API_MESSAGE = "RuntimeContext created using deprecated constructor. "
            + "For support use RuntimeContext() and RuntimeContext(RuntimeContext)";

    // In 5.5 RuntimeContext was an immutable container that modelled a single StatefulSet.
    // When you create a RuntimeContext using the deprecated constructor (this was the only way <= 5.5)
    // the RuntimeContext models a single StatefulSet and uses the following to model that StatefulSet.
    // All APIs added post 5.5 are unsupported.
    private static final class RuntimeContextImmutable {
        private final int specifiedReplicaCount;
        private final int readyReplicas;
        private final int currentReplicas;
        private final @Nullable String resourceVersion;

        private RuntimeContextImmutable(int specifiedReplicaCount,
                                        int readyReplicas,
                                        int currentReplicas,
                                        @Nullable String resourceVersion) {
            this.specifiedReplicaCount = specifiedReplicaCount;
            this.readyReplicas = readyReplicas;
            this.currentReplicas = currentReplicas;
            this.resourceVersion = resourceVersion;
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

    private final Map<String, StatefulSetInfo> statefulSets = new HashMap<>();
    private StatefulSetInfo merged;
    private String resourceVersion;

    // null if the RuntimeContext was instantiated by a constructor added post 5.5
    @Nullable
    private final RuntimeContextImmutable immutableContext;

    /**
     * Creates a {@link #RuntimeContext} that can track the state of multiple StatefulSets.
     * The created {@link #RuntimeContext} is not thread-safe.
     * @since 5.6
     */
    public RuntimeContext() {
        immutableContext = null;
     }

    /**
     * Creates a {@link #RuntimeContext} whose state is derived from another {@link #RuntimeContext}.
     * The created {@link #RuntimeContext} is not thread-safe.
     * @throws UnsupportedOperationException when copying a {@link #RuntimeContext} created using
     * {@link #RuntimeContext(int, int, int, String)}.
     * @since 5.6
     */
    public RuntimeContext(RuntimeContext context) {
        if (context.immutableContext != null) {
            throw new UnsupportedOperationException("Copying the state of a RuntimeContext created"
                + " using a deprecated API is not permitted");
        }

        statefulSets.putAll(context.statefulSets);
        merged = context.merged;
        resourceVersion = context.resourceVersion;

        immutableContext = null;
    }

    /**
     * @deprecated Use {@link #RuntimeContext()} and {@link #addStatefulSetInfo(String, StatefulSetInfo, String)} instead.
     *             This constructor is provided for backward compatibility only.
     * <p>
     * Creates a {@link #RuntimeContext} that can model a single StatefulSet.
     * The created {@link #RuntimeContext} is thread-safe.
     * The view of state returned by this constructor supports the following API:
     * </p>
     * <ul>
     *   <li>{@link #getCurrentReplicas()}</li>
     *   <li>{@link #getReadyReplicas()}</li>
     *   <li>{@link #getSpecifiedReplicaCount()}</li>
     *   <li>{@link #getResourceVersion()}</li>
     *   <li>{@link #toString()}</li>
     * </ul>
     * <p>All other method invocations will throw {@link UnsupportedOperationException}.</p>
     */
    @Deprecated(forRemoval = true, since = "5.6")
    public RuntimeContext(int specifiedReplicaCount, int readyReplicas, int currentReplicas, @Nullable String resourceVersion) {
        immutableContext = new RuntimeContextImmutable(specifiedReplicaCount,
                                                              readyReplicas,
                                                              currentReplicas,
                                                              resourceVersion);
    }

    private boolean isLegacy() {
        return immutableContext != null;
    }

    /**
     * @param specifiedReplicaCount specified number of replicas, corresponds to {@code StatefulSetSpec.replicas}
     * @param readyReplicas         number of ready replicas, corresponds to {@code StatefulSetStatus.readyReplicas}
     * @param currentReplicas       number of replicas created by the current revision of the StatefulSet,
     *                              corresponds to {@code StatefulSetStatus.currentReplicas}
     * @since 5.6
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
                .reduce((a, b) -> a == ClusterTopologyIntentTracker.UNKNOWN || b == ClusterTopologyIntentTracker.UNKNOWN
                                          ? ClusterTopologyIntentTracker.UNKNOWN : a + b)
                .orElse(ClusterTopologyIntentTracker.UNKNOWN);
    }

    /**
     * Returns the total number of specified replicas. If the context
     * is empty, returns {@value ClusterTopologyIntentTracker#UNKNOWN}.
     *
     * @see StatefulSetInfo#specifiedReplicaCount()
     */
    public int getSpecifiedReplicaCount() {
        return isLegacy() ? immutableContext.specifiedReplicaCount : getMerged().specifiedReplicaCount;
    }

    /**
     * Returns the total number of ready replicas. If the context
     * is empty, returns {@value ClusterTopologyIntentTracker#UNKNOWN}.
     *
     * @see StatefulSetInfo#readyReplicas()
     */
    public int getReadyReplicas() {
        return isLegacy() ? immutableContext.readyReplicas : getMerged().readyReplicas;
    }

    /**
     * Returns the total number of current replicas. If the context
     * is empty, returns {@value ClusterTopologyIntentTracker#UNKNOWN}.
     *
     * @see StatefulSetInfo#currentReplicas()
     */
    public int getCurrentReplicas() {
        return isLegacy() ? immutableContext.currentReplicas : getMerged().currentReplicas;
    }

    /** Returns the last resource version specified via {@link #addStatefulSetInfo}. */
    public String getResourceVersion() {
        return isLegacy() ? immutableContext.resourceVersion : resourceVersion;
    }

    /**
     * Returns the number of StatefulSets tracked by this context.
     * @return the number of StatefulSets
     * @throws UnsupportedOperationException when the {@link #RuntimeContext} was created
     * using {@link #RuntimeContext(int, int, int, String)}.
     * @since 5.6
     */
    public int getStatefulSetCount() {
        if (isLegacy()) {
            // pessimistically done because we don't want the legacy semantics to be
            // inadvertently used in contexts where the >= 5.6 semantics is expected
            throw new UnsupportedOperationException(UNSUPPORTED_API_MESSAGE);
        }
        return statefulSets.size();
    }

    /**
     * Adds or updates StatefulSet information in this context.
     * @param name the name of the StatefulSet
     * @param info the StatefulSet information
     * @param resourceVersion the resource version
     * @throws UnsupportedOperationException when the {@link #RuntimeContext} was created
     * using {@link #RuntimeContext(int, int, int, String)}.
     * @since 5.6
     */
    public void addStatefulSetInfo(String name, @Nonnull StatefulSetInfo info, String resourceVersion) {
        if (isLegacy()) {
            throw new UnsupportedOperationException(UNSUPPORTED_API_MESSAGE);
        }
        StatefulSetInfo previous = statefulSets.put(name, info);
        if (!info.equals(previous)) {
            merged = null;
        }
        this.resourceVersion = resourceVersion;
    }

    @Override
    public String toString() {
        if (isLegacy()) {
            return immutableContext.toString();
        }
        return "RuntimeContext{"
                + "specifiedReplicaCount=" + getSpecifiedReplicaCount()
                + ", readyReplicas=" + getReadyReplicas()
                + ", currentReplicas=" + getCurrentReplicas()
                + ", resourceVersion='" + resourceVersion + '\''
                + '}';
    }
}
