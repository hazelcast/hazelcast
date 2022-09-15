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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.ProbeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static java.util.Objects.requireNonNull;

/**
 * Default implementation of {@link MetricDescriptor}.
 */
@SuppressWarnings("checkstyle:MethodCount")
public final class MetricDescriptorImpl implements MetricDescriptor {
    private static final int INITIAL_TAG_CAPACITY = 4;
    private static final double GROW_FACTOR = 1.2D;
    private static final int INITIAL_STRING_CAPACITY = 64;

    private final LookupView lookupView = new LookupView();

    private Supplier<MetricDescriptorImpl> supplier;
    private String[] tags;
    private int tagPtr;
    private String prefix;
    private String metric;
    private String discriminator;
    private String discriminatorValue;
    private ProbeUnit unit;
    private Collection<MetricTarget> excludedTargets = MetricTarget.NONE_OF;

    public MetricDescriptorImpl(Supplier<MetricDescriptorImpl> supplier) {
        this.supplier = requireNonNull(supplier);

        tags = new String[INITIAL_TAG_CAPACITY * 2];
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withTag(String tag, String value) {
        ensureCapacity(tagPtr);

        tags[tagPtr] = requireNonNull(tag);
        tags[tagPtr + 1] = requireNonNull(value);
        tagPtr += 2;
        return this;
    }

    @Nullable
    @Override
    public String tagValue(String tag) {
        Objects.requireNonNull(tag);
        for (int i = 0; i < tagPtr; i += 2) {
            String tagStored = tags[i];
            String tagValue = tags[i + 1];
            if (tag.equals(tagStored)) {
                return tagValue;
            }
        }
        return null;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withMetric(String metric) {
        this.metric = metric;
        return this;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withDiscriminator(String discriminatorTag, String discriminatorValue) {
        this.discriminator = discriminatorTag;
        this.discriminatorValue = discriminatorValue;
        return this;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withUnit(ProbeUnit unit) {
        this.unit = unit;
        return this;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl copy() {
        MetricDescriptorImpl copy = supplier.get();
        copy.prefix = prefix;
        copy.metric = metric;
        copy.discriminator = discriminator;
        copy.discriminatorValue = discriminatorValue;
        copy.unit = unit;
        copy.excludedTargets = excludedTargets;
        copy.ensureCapacity(tagCount() << 1);
        readTags(copy::withTag);

        return copy;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl copy(MetricDescriptor descriptor) {
        reset();

        this.prefix = descriptor.prefix();
        this.metric = descriptor.metric();
        this.discriminator = descriptor.discriminator();
        this.discriminatorValue = descriptor.discriminatorValue();
        this.unit = descriptor.unit();
        this.excludedTargets = descriptor.excludedTargets();
        this.ensureCapacity(descriptor.tagCount() << 1);
        descriptor.readTags(this::withTag);

        return this;
    }

    @Nullable
    @Override
    public String prefix() {
        return prefix;
    }

    @Override
    public String metric() {
        return metric;
    }

    @Nullable
    @Override
    public String discriminator() {
        return discriminator;
    }

    @Nullable
    @Override
    public String discriminatorValue() {
        return discriminatorValue;
    }

    @Nullable
    @Override
    public ProbeUnit unit() {
        return unit;
    }

    @Override
    public int tagCount() {
        return tagPtr >> 1;
    }

    @Override
    public void readTags(BiConsumer<String, String> tagReader) {
        for (int i = 0; i < tagPtr; i += 2) {
            String tag = tags[i];
            String tagValue = tags[i + 1];
            tagReader.accept(tag, tagValue);
        }
    }

    @Override
    public String tag(int index) {
        index = index << 1;
        if (index < 0 || index >= tagPtr) {
            throw new IndexOutOfBoundsException();
        }

        return tags[index];
    }

    @Override
    public String tagValue(int index) {
        index = (index << 1) + 1;
        if (index < 0 || index >= tagPtr) {
            throw new IndexOutOfBoundsException();
        }

        return tags[index];
    }

    @Override
    @Nonnull
    public String metricString() {
        return buildMetricString(false);
    }

    private String buildMetricString(boolean includeExcludedTargets) {
        StringBuilder sb = new StringBuilder(INITIAL_STRING_CAPACITY).append('[');

        if (discriminatorValue != null) {
            sb.append(discriminator).append('=').append(discriminatorValue).append(',');
        }

        if (unit != null) {
            sb.append("unit=").append(lowerCaseInternal(unit.name())).append(',');
        }

        if (metric != null) {
            sb.append("metric=");
            if (prefix != null) {
                sb.append(prefix).append('.');
            }
            sb.append(metric);
            sb.append(',');
        }

        for (int i = 0; i < tagPtr; i += 2) {
            String tag = tags[i];
            String tagValue = tags[i + 1];
            sb.append(tag).append('=').append(tagValue).append(',');
        }

        if (includeExcludedTargets) {
            sb.append("excludedTargets={");
            if (excludedTargets.isEmpty()) {
                sb.append('}');
            } else {
                for (MetricTarget target : excludedTargets) {
                    sb.append(target.name()).append(',');
                }
                sb.setCharAt(sb.length() - 1, '}');
            }
            sb.append(',');
        }

        if (sb.length() > 1) {
            sb.setCharAt(sb.length() - 1, ']');
        } else {
            sb.append(']');
        }

        return sb.toString();
    }

    @Nonnull
    @Override
    public Collection<MetricTarget> excludedTargets() {
        return excludedTargets;
    }

    @Override
    public boolean isTargetExcluded(MetricTarget target) {
        return excludedTargets.contains(target);
    }

    @Override
    public boolean isTargetIncluded(MetricTarget target) {
        return !isTargetExcluded(target);
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withExcludedTarget(MetricTarget target) {
        excludedTargets = MetricTarget.asSetWith(excludedTargets, target);
        return this;
    }

    @Override
    @Nonnull
    public MetricDescriptorImpl withIncludedTarget(MetricTarget target) {
        excludedTargets = MetricTarget.asSetWithout(excludedTargets, target);
        return this;
    }

    @Nonnull
    @Override
    public MetricDescriptorImpl withExcludedTargets(Collection<MetricTarget> excludedTargets) {
        this.excludedTargets = excludedTargets;
        return this;
    }

    LookupView lookupView() {
        return lookupView;
    }

    private void ensureCapacity(int tagPtr) {
        if (tagPtr < tags.length) {
            return;
        }

        int newCapacity = (int) Math.max(tagPtr, Math.ceil(tags.length * GROW_FACTOR));
        if (newCapacity % 2 != 0) {
            newCapacity++;
        }
        String[] newTags = new String[newCapacity];
        System.arraycopy(tags, 0, newTags, 0, tags.length);
        tags = newTags;
    }

    @Override
    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetricDescriptorImpl that = (MetricDescriptorImpl) o;

        if (tagPtr != that.tagPtr) {
            return false;
        }
        if (unit != that.unit) {
            return false;
        }
        if (!Objects.equals(prefix, that.prefix)) {
            return false;
        }
        if (!Objects.equals(metric, that.metric)) {
            return false;
        }
        if (!Objects.equals(discriminatorValue, that.discriminatorValue)) {
            return false;
        }
        if (!Objects.equals(this.discriminator, that.discriminator)) {
            return false;
        }
        if (!Objects.equals(this.excludedTargets, that.excludedTargets)) {
            return false;
        }

        // since we already checked that the two descriptors have the same number
        // of tags, we can safely compare them from only one side but we need
        // to compare pairs. The order of tags doesn't matter.
        outer:
        for (int i = 0; i < tagPtr; i += 2) {
            String thisTag = tags[i];
            String thisTagValue = tags[i + 1];
            for (int j = 0; j < that.tagPtr; j += 2) {
                String thatTag = that.tags[j];
                String thatTagValue = that.tags[j + 1];
                if (thisTag.equals(thatTag) && thisTagValue.equals(thatTagValue)) {
                    continue outer;
                }
            }
            // no match
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (int i = 0; i < tagPtr; i += 2) {
            String tag = tags[i];
            String tagValue = tags[i + 1];
            result += tag.hashCode() + 31 * tagValue.hashCode();
        }

        result = 31 * result + tagPtr;
        result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
        result = 31 * result + (metric != null ? metric.hashCode() : 0);
        result = 31 * result + (discriminator != null ? discriminator.hashCode() : 0);
        result = 31 * result + (discriminatorValue != null ? discriminatorValue.hashCode() : 0);
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        result = 31 * result + excludedTargets.hashCode();

        return result;
    }

    @Override
    @Nonnull
    public MetricDescriptor reset() {
        prefix = null;
        metric = null;
        unit = null;
        tagPtr = 0;
        discriminator = null;
        discriminatorValue = null;
        Arrays.fill(tags, null);

        return this;
    }

    /**
     * Sets the supplier to the given one. Used by
     * {@link PoolingMetricDescriptorSupplier#close()} to ensure no leaking
     * is possible.
     *
     * @param supplier The supplier to set.
     */
    void setSupplier(Supplier<MetricDescriptorImpl> supplier) {
        this.supplier = requireNonNull(supplier);
    }

    @Override
    public String toString() {
        return buildMetricString(true);
    }

    /**
     * Reduced view of the {@link MetricDescriptorImpl}. Used for looking
     * up {@link Gauge}s.
     */
    public class LookupView {

        public MetricDescriptorImpl descriptor() {
            return MetricDescriptorImpl.this;
        }

        private String prefix() {
            return prefix;
        }

        private String metricName() {
            return metric;
        }

        private String discriminatorValue() {
            return discriminatorValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LookupView that = (LookupView) o;

            if (!Objects.equals(prefix, that.prefix())) {
                return false;
            }
            if (!Objects.equals(metric, that.metricName())) {
                return false;
            }
            return Objects.equals(discriminatorValue, that.discriminatorValue());
        }

        @Override
        public int hashCode() {
            int result = prefix != null ? prefix.hashCode() : 0;
            result = 31 * result + (metric != null ? metric.hashCode() : 0);
            result = 31 * result + (discriminatorValue != null ? discriminatorValue.hashCode() : 0);
            result = 31 * result + (discriminatorValue != null ? discriminatorValue.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return metricName();
        }
    }
}
