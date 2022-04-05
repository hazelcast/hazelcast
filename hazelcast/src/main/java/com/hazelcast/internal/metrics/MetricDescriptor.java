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

package com.hazelcast.internal.metrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;

/**
 * Mutable interface for describing a metric.
 */
public interface MetricDescriptor {
    /**
     * Sets the prefix of the descriptor's metric.
     *
     * @param prefix The prefix to set
     * @return the descriptor instance
     * @see MetricDescriptor#prefix()
     */
    @Nonnull
    MetricDescriptor withPrefix(String prefix);

    /**
     * Returns the prefix of the metric denoted by this instance.
     *
     * @return the prefix
     */
    @Nullable
    String prefix();

    /**
     * Sets the metric field of the descriptor.
     *
     * @param metric The metric to set
     * @return the descriptor instance
     * @see MetricDescriptor#metric()
     */
    @Nonnull
    MetricDescriptor withMetric(String metric);

    /**
     * Returns the name of the metric denoted by this instance.
     *
     * @return the name
     */
    String metric();

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
    MetricDescriptor withDiscriminator(String discriminatorTag, String discriminatorValue);

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
     * Sets the unit of the descriptor.
     *
     * @param unit The unit to set
     * @return the descriptor instance
     * @see  interface for describing a metric#unit()
     */
    @Nonnull
    MetricDescriptor withUnit(ProbeUnit unit);

    /**
     * Returns the unit of the metric denoted by this instance.
     *
     * @return the unit
     */
    @Nullable
    ProbeUnit unit();

    /**
     * Adds the given tag to the descriptor with the given value.
     *
     * @param tag   The tag to add
     * @param value The value of the tag
     * @return the descriptor instance
     * @see MetricDescriptor#readTags(BiConsumer)
     */
    @Nonnull
    MetricDescriptor withTag(String tag, String value);

    /**
     * Returns the value associated with the given {@code tag} or {@code null}
     * if the given tag is not set for the descriptor. If tag is duplicated,
     * returns the first occurrence.
     *
     * @param tag The tag
     * @return the value of the tag or {@code null}
     */
    @Nullable
    String tagValue(String tag);

    /**
     * Calls the given {@code tagReader} with all tags in this descriptor.
     *
     * @param tagReader The reader to call
     * @see #tag(int)
     * @see #tagValue(int)
     */
    void readTags(BiConsumer<String, String> tagReader);

    /**
     * Returns the name of the tag at the given index.
     *
     * @param index The index
     * @see #tagValue(int)
     * @see #tagCount()
     * @see #readTags(BiConsumer)
     * @throws IndexOutOfBoundsException if index is out of bounds
     */
    @Nullable
    String tag(int index);

    /**
     * Returns the value of the tag at the given index.
     *
     * @param index The index
     * @see #tag(int)
     * @see #tagCount()
     * @see #readTags(BiConsumer)
     * @throws IndexOutOfBoundsException if index is out of bounds
     */
    @Nullable
    String tagValue(int index);

    /**
     * Returns the number of tags in this descriptor.
     *
     * @return the number of tags
     */
    int tagCount();

    /**
     * Returns a textual representation of this descriptor.
     * <p>
     * Note: The excluded targets are not part of the textual
     * representation of the metric returned by this method, but
     * part of its {@link #toString()}.
     *
     * @return the textual representation
     */
    @Nonnull
    String metricString();

    /**
     * Returns the excluded targets.
     *
     * @return the excluded targets
     */
    @Nonnull
    Collection<MetricTarget> excludedTargets();

    /**
     * Excludes the given target for this metric.
     * <p>
     * Note: The excluded targets are not part of the textual
     * representation of the metric returned by {@link #metricString()}, but
     * part of its {@link #toString()}.
     *
     * @param target The target to exclude
     * @return the descriptor instance
     * @see #withIncludedTarget(MetricTarget)
     * @see MetricDescriptor#isTargetExcluded(MetricTarget)
     * @see MetricDescriptor#isTargetIncluded(MetricTarget)
     */
    @Nonnull
    MetricDescriptor withExcludedTarget(MetricTarget target);

    /**
     * Excludes the given targets for this metric.
     * <p>
     * Note1: Calling this method overwrites the previously set excluded targets
     * Note2: The excluded targets are not part of the textual
     * representation of the metric returned by {@link #metricString()}, but
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
    MetricDescriptor withExcludedTargets(Collection<MetricTarget> excludedTargets);

    /**
     * Includes the given target for this metric.
     * <p>
     * Note1: This is the invert method for {@link #withExcludedTarget(MetricTarget)}
     * Note2: The excluded targets are not part of the textual
     * representation of the metric returned by {@link #metricString()}, but
     * part of its {@link #toString()}.
     *
     * @param target The target to exclude
     * @return the descriptor instance
     * @see #withExcludedTarget(MetricTarget)
     * @see MetricDescriptor#isTargetExcluded(MetricTarget)
     * @see MetricDescriptor#isTargetIncluded(MetricTarget)
     */
    @Nonnull
    MetricDescriptor withIncludedTarget(MetricTarget target);

    /**
     * Returns {@code true} if this metric should be excluded on the given
     * {@link MetricTarget}
     *
     * @param target The target to check
     * @return {@code true} if the metric should be excluded on the target,
     * {@code false} otherwise
     */
    boolean isTargetExcluded(MetricTarget target);

    /**
     * Returns {@code true} if this metric should be included on the given
     * {@link MetricTarget}
     *
     * @param target The target to check
     * @return {@code true} if the metric should be included on the target,
     * {@code false} otherwise
     */
    boolean isTargetIncluded(MetricTarget target);

    /**
     * Takes a mutable copy of this instance.
     *
     * @return the copy of this instance
     */
    @Nonnull
    MetricDescriptor copy();

    /**
     * Copies the given {@code descriptor}'s fields into this mutable
     * descriptor.
     *
     * @param descriptor The descriptor to copy
     * @return the this instance containing the fields of the
     * {@code descriptor} argument
     */
    @Nonnull
    MetricDescriptor copy(MetricDescriptor descriptor);

    /**
     * Resets the descriptor instance
     *
     * @return the this instance after reset
     */
    @Nonnull
    MetricDescriptor reset();
}
