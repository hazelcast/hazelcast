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

package com.hazelcast.internal.services;

/**
 * An interface that can be implemented by SPI services that want to be able to resolve a split-brain.
 * <p>
 * When the two separate clusters merge, the {@link #prepareMergeRunnable()} method is called to return
 * a {@link Runnable}, that will merge the clusters.
 */
public interface SplitBrainHandlerService {

    /**
     * When the two separate clusters merge (resolve a split-brain), this method is called to return
     * a {@link Runnable}, that will merge the clusters.
     *
     * @return a {@link Runnable} that will merge the clusters
     */
    Runnable prepareMergeRunnable();
}
