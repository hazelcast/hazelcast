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

import com.hazelcast.internal.metrics.CollectionCycle.Tags;
import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Can be implemented by instances passed to
 * {@link CollectionCycle#collectAll(Object)} to provide the {@link Tags}
 * context through the probed object itself instead building it on the level
 * above.
 */
@PrivateApi
public interface ObjectMetricsContext {

    /**
     * The implementing object (with {@link Probe} annotated fields or methods) is
     * asked to provide the {@link Tags} context for the object.
     *
     * The performed tagging is relative to potential parent context {@link Tags}
     * that were added before probing the implementing instance.
     *
     * This is an alternative to building the context in the
     * {@link MetricsSource#collectAll(CollectionCycle)} implementation. This is for
     * example useful when the probed object is an abstraction and the context
     * relies on private information that cannot be accessed or that is different
     * for different implementation classes.
     *
     * @param context to use to build the objects context using the {@link Tags}
     *        methods.
     */
    void switchToObjectContext(Tags context);
}
