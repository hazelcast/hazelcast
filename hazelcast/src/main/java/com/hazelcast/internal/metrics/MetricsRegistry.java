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

package com.hazelcast.internal.metrics;

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * A service made accessible to core services so they have a chance to register
 * the "root objects" of the system.
 *
 * There is no need to unregister them later. Filtered
 * {@link #openContext(Class...)} can be used to restrict used
 * {@link MetricsSource}s.
 *
 * Metrics about things that change over time require a corresponding
 * {@link MetricsSource} (existing since the startup) that does probe the
 * available instances as they appear and stops doing that as they disappear.
 */
@PrivateApi
public interface MetricsRegistry {

    /**
     * Called once at startup, typically by a core service registering itself.
     *
     * @param source a object that "knows" how to make metrics known to a
     *        {@link CollectionCycle} in their appropriate context. Each sources is
     *        assumed to be unique per class. That means a second registration of a
     *        source is considered identical and therefore unnecessary if the source
     *        has the same type as an already registered source. Such a source
     *        registration is simply ignored. This is the most practical behavior as
     *        we usually do not want same source more then once.
     */
    void register(MetricsSource source);

    /**
     * Legacy support instances only known by their interface with implementations
     * that possibly implement {@link MetricsSource}.
     *
     * @param source a object possibly implementing {@link MetricsSource}, might be
     *        null as well
     */
    @Deprecated
    void registerIfSource(Object source);

    /**
     * Creates a new "private "context that should be kept by the caller to collect
     * the contents of this {@link MetricsRegistry}.
     *
     * The implementation will not support multi-threading as each thread should
     * create its own context instance.
     *
     * @param selection the set of sources that is accepted (kept) if it was or will
     *        be registered. A empty set or {@code null} results in usage of all
     *        registered sources.
     * @return a new private selective "context". The context is updated when
     *         further {@link MetricsSource} are {@link #register(MetricsSource)}ed that
     *         were selected.
     */
    CollectionContext openContext(Class<? extends MetricsSource>... selection);
}
