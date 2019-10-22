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

package com.hazelcast.internal.metrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiConsumer;

/**
 * Read-only interface for describing a metric.
 *
 * @see MutableMetricDescriptor
 */
public interface MetricDescriptor {
    /**
     * Returns the prefix of the metric denoted by this instance.
     *
     * @return the prefix
     */
    @Nullable
    String prefix();

    /**
     * Returns the name of the metric denoted by this instance.
     *
     * @return the name
     */
    @Nonnull
    String metric();

    /**
     * Returns the discriminator tag's name of the metric denoted by
     * this instance. Used to distinguish metrics that have the same
     * {@link #prefix()} and {@link #metric()} from each other.
     *
     * @return the discriminator tag
     * @see #discriminatorValue()
     */
    @Nullable
    String discriminator();

    /**
     * Returns the discriminator tag's value of the metric denoted by
     * this instance. Used to distinguish metrics that have the same
     * {@link #prefix()} and {@link #metric()} from each other.
     *
     * @return the discriminator tag
     * @see #discriminator()
     */
    @Nullable
    String discriminatorValue();

    /**
     * Returns the unit of the metric denoted by this instance.
     *
     * @return the unit
     */
    @Nullable
    ProbeUnit unit();

    /**
     * Calls the given {@code tagReader} with all tags in this descriptor.
     *
     * @param tagReader The reader to call
     */
    void readTags(BiConsumer<String, String> tagReader);

    /**
     * Returns the number of tags in this descriptor.
     *
     * @return the number of tags
     */
    int tagCount();

    /**
     * Returns a textual representation of this descriptor.
     *
     * @return the textual representation
     */
    @Nonnull
    String metricName();
}
