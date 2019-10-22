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
import java.util.function.BiConsumer;

/**
 * Interface to be implemented by mutable {@link MetricDescriptor}s.
 */
public interface MutableMetricDescriptor extends MetricDescriptor {

    /**
     * Sets the prefix of the descriptor's metric.
     *
     * @param prefix The prefix to set
     * @return the descriptor instance
     * @see MetricDescriptor#prefix()
     */
    @Nonnull
    MutableMetricDescriptor withPrefix(String prefix);

    /**
     * Sets the metric field of the descriptor.
     *
     * @param metric The metric to set
     * @return the descriptor instance
     * @see MetricDescriptor#metric()
     */
    @Nonnull
    MutableMetricDescriptor withMetric(String metric);

    /**
     * Sets the discriminator tag of the descriptor.
     *
     * @param discriminatorTag   The name of the discriminator tag
     * @param discriminatorValue The value of the discriminator tag
     * @return the descriptor instance
     * @see MetricDescriptor#discriminator()
     * @see MetricDescriptor#discriminatorValue()
     */
    @Nonnull
    MutableMetricDescriptor withDiscriminator(String discriminatorTag, String discriminatorValue);

    /**
     * Sets the unit of the descriptor.
     *
     * @param unit The unit to set
     * @return the descriptor instance
     * @see MetricDescriptor#unit()
     */
    @Nonnull
    MutableMetricDescriptor withUnit(ProbeUnit unit);

    /**
     * Adds the given tag to the descriptor with the given value.
     *
     * @param tag   The tag to add
     * @param value The value of the tag
     * @return the descriptor instance
     * @see MetricDescriptor#readTags(BiConsumer)
     */
    @Nonnull
    MutableMetricDescriptor withTag(String tag, String value);

    /**
     * Takes a mutable copy of this instance.
     *
     * @return the copy of this instance
     */
    @Nonnull
    MutableMetricDescriptor copy();

    /**
     * Copies the given {@code descriptor}'s fields into this mutable
     * descriptor.
     *
     * @param descriptor The descriptor to copy
     * @return the this instance containing the fields of the
     * {@code descriptor} argument
     */
    @Nonnull
    MutableMetricDescriptor copy(MetricDescriptor descriptor);
}
