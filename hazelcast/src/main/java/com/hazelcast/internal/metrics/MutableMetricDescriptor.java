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
import java.util.Collection;
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
     * Excludes the given targets for this metric.
     * <p>
     * Note1: Calling this method overwrites the previously set excluded targets
     * Note2: The excluded targets are not part of the textual
     * representation of the metric returned by {@link #metricName()}, but
     * part of its {@link #toString()}.
     *
     * @param excludedTargets The targets to exclude
     * @return the descriptor instance
     * @see #withExcludedTarget(MetricTarget)
     * @see #withIncludedTarget(MetricTarget)
     * @see MetricDescriptor#isTargetExcluded(MetricTarget)
     * @see MetricDescriptor#isTargetIncluded(MetricTarget)
     */
    @Nonnull
    MutableMetricDescriptor withExcludedTargets(Collection<MetricTarget> excludedTargets);

    /**
     * Excludes the given target for this metric.
     * <p>
     * Note: The excluded targets are not part of the textual
     * representation of the metric returned by {@link #metricName()}, but
     * part of its {@link #toString()}.
     *
     * @param target The target to exclude
     * @return the descriptor instance
     * @see #withIncludedTarget(MetricTarget)
     * @see MetricDescriptor#isTargetExcluded(MetricTarget)
     * @see MetricDescriptor#isTargetIncluded(MetricTarget)
     */
    @Nonnull
    MutableMetricDescriptor withExcludedTarget(MetricTarget target);

    /**
     * Includes the given target for this metric.
     * <p>
     * Note1: This is the invert method for {@link #withExcludedTarget(MetricTarget)}
     * Note2: The excluded targets are not part of the textual
     * representation of the metric returned by {@link #metricName()}, but
     * part of its {@link #toString()}.
     *
     * @param target The target to exclude
     * @return the descriptor instance
     * @see #withExcludedTarget(MetricTarget)
     * @see MetricDescriptor#isTargetExcluded(MetricTarget)
     * @see MetricDescriptor#isTargetIncluded(MetricTarget)
     */
    @Nonnull
    MutableMetricDescriptor withIncludedTarget(MetricTarget target);

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
