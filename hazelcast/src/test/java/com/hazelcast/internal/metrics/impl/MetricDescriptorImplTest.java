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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EnumSet;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricDescriptorImplTest {

    private Supplier<MetricDescriptorImpl> supplier;

    @Before
    public void setUp() {
        supplier = new Supplier<MetricDescriptorImpl>() {
            @Override
            public MetricDescriptorImpl get() {
                return new MetricDescriptorImpl(this);
            }
        };
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullThrows() {
        new MetricDescriptorImpl(null);
    }

    @Test
    public void testMetricName_withPrefixAndId() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withMetric("metricValue");

        assertEquals("[discriminatorTag=discriminatorValue,metric=prefix.metricValue]", descriptor.metricName());
    }

    @Test
    public void testMetricName_withPrefixWithoutId() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withUnit(ProbeUnit.PERCENT);

        assertEquals("[unit=percent,metric=prefix.metricValue]", descriptor.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixWithId() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withMetric("metricValue");

        assertEquals("[discriminatorTag=discriminatorValue,metric=metricValue]", descriptor.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixAndId() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withMetric("metricValue");

        assertEquals("[metric=metricValue]", descriptor.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixAndId_withTags() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withMetric("metricValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value")
                .withTag("tag2", "tag2Value")
                .withTag("tag3", "tag3Value")
                .withTag("tag4", "tag4Value")
                .withTag("tag5", "tag5Value");

        assertEquals("[unit=ms,metric=metricValue,tag0=tag0Value,tag1=tag1Value,tag2=tag2Value,tag3=tag3Value,tag4=tag4Value,"
                + "tag5=tag5Value]", descriptor.metricName());
    }

    @Test
    public void testMetricName_withPrefixAndId_withTags() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value")
                .withTag("tag2", "tag2Value")
                .withTag("tag3", "tag3Value")
                .withTag("tag4", "tag4Value")
                .withTag("tag5", "tag5Value");

        assertEquals(
                "[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metricValue,tag0=tag0Value,tag1=tag1Value,tag2=tag2Value,"
                        + "tag3=tag3Value,tag4=tag4Value,tag5=tag5Value]", descriptor.metricName());
    }

    @Test
    public void testMetricName_withPrefixAndId_withSingleTag() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value");

        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metricValue,tag0=tag0Value]",
                descriptor.metricName());
    }

    @Test
    public void testCopy_withMetricName_withPrefixAndId_withTags() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value")
                .withTag("tag2", "tag2Value");

        MetricDescriptorImpl descriptorCopy = descriptor
                .copy()
                .withMetric("metric2")
                .withTag("tag3", "tag3Value")
                .withTag("tag4", "tag4Value")
                .withTag("tag5", "tag5Value");

        descriptor.withMetric("metric1");

        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metric1,tag0=tag0Value,tag1=tag1Value,"
                + "tag2=tag2Value]", descriptor.metricName());
        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metric2,tag0=tag0Value,tag1=tag1Value,"
                + "tag2=tag2Value,tag3=tag3Value,tag4=tag4Value,tag5=tag5Value]", descriptorCopy.metricName());
    }

    @Test
    public void testMetricName_doesntIncludeExcludedTargets() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value")
                .withExcludedTarget(MANAGEMENT_CENTER);

        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metricValue,tag0=tag0Value]",
                descriptor.metricName());
    }

    @Test
    public void testToString_includesExcludedTargets() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value")
                .withExcludedTarget(MANAGEMENT_CENTER)
                .withExcludedTarget(JMX);

        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metricValue,tag0=tag0Value,"
                        + "excludedTargets={MANAGEMENT_CENTER,JMX}]",
                descriptor.toString());
    }

    @Test
    public void testEqualsDifferentNumberOfTags() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withPrefix("prefix")
                .withMetric("metricName")
                .withTag("tag0", "tag0Value");

        metricDescriptor2
                .withPrefix("prefix")
                .withMetric("metricName")
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentUnits() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withUnit(COUNT);

        metricDescriptor2
                .withMetric("metricName")
                .withUnit(BYTES);

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentPrefixes() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withPrefix("prefix")
                .withMetric("metricName");

        metricDescriptor2
                .withPrefix("otherPrefix")
                .withMetric("metricName");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentMetricNames() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1.withMetric("metricName");

        metricDescriptor2.withMetric("otherMetricName");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentDiscriminatorValues() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withDiscriminator("disc", "discValue");

        metricDescriptor2
                .withMetric("metricName")
                .withDiscriminator("disc", "otherDiscValue");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentDiscriminatorTags() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withDiscriminator("disc", "discValue");

        metricDescriptor2
                .withMetric("metricName")
                .withDiscriminator("otherDisc", "discValue");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentTags() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withTag("tag", "tagValue");

        metricDescriptor2
                .withMetric("metricName")
                .withTag("otherTag", "tagValue");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentTagValues() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withTag("tag", "tagValue");

        metricDescriptor2
                .withMetric("metricName")
                .withTag("tag", "otherTagValue");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentClass() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(mock(Supplier.class));
        assertNotEquals(descriptor, new Object());
    }

    @Test
    public void testEqualsSameDescriptor() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(mock(Supplier.class));
        assertEquals(descriptor, descriptor);
    }

    @Test
    public void testEqualsSameExcludedTargets() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withExcludedTarget(MANAGEMENT_CENTER)
                .withExcludedTarget(JMX);

        metricDescriptor2
                .withMetric("metricName")
                .withExcludedTarget(JMX)
                .withExcludedTarget(MANAGEMENT_CENTER);

        assertEquals(metricDescriptor1, metricDescriptor2);
        assertEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentExcludedTargets() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withExcludedTarget(MANAGEMENT_CENTER);

        metricDescriptor2
                .withMetric("metricName")
                .withExcludedTarget(JMX)
                .withExcludedTarget(MANAGEMENT_CENTER);

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testCopyOfAnotherDescriptor() {
        MetricDescriptorImpl original = new MetricDescriptorImpl(mock(Supplier.class))
                .withPrefix("prefix")
                .withMetric("metric")
                .withDiscriminator("discriminator", "discriminatorValue")
                .withUnit(PERCENT)
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value")
                .withTag("tag2", "tag2Value")
                .withTag("tag3", "tag3Value")
                .withTag("tag4", "tag4Value")
                .withExcludedTarget(JMX);
        MetricDescriptorImpl target = new MetricDescriptorImpl(mock(Supplier.class))
                .copy(original);

        assertEquals(original, target);
    }

    @Test
    public void testTargetExclusion() {
        MetricDescriptorImpl original = new MetricDescriptorImpl(mock(Supplier.class))
                .withExcludedTarget(MANAGEMENT_CENTER)
                .withExcludedTarget(JMX);

        assertTrue(original.isTargetExcluded(MANAGEMENT_CENTER));
        assertTrue(original.isTargetExcluded(JMX));

        EnumSet<MetricTarget> expectedIncludedTargets = EnumSet.complementOf(EnumSet.of(MANAGEMENT_CENTER, JMX));
        for (MetricTarget metricTarget : expectedIncludedTargets) {
            assertFalse(original.isTargetExcluded(metricTarget));
        }
    }

    @Test
    public void testExcludeThanIncludeIncludesTarget() {
        MetricDescriptorImpl original = new MetricDescriptorImpl(mock(Supplier.class))
                .withExcludedTarget(MANAGEMENT_CENTER)
                .withIncludedTarget(MANAGEMENT_CENTER);

        assertFalse(original.isTargetExcluded(MANAGEMENT_CENTER));
        assertTrue(original.isTargetIncluded(MANAGEMENT_CENTER));
    }

    @Test
    public void testIncludeThanExcludeExcludesTarget() {
        MetricDescriptorImpl original = new MetricDescriptorImpl(mock(Supplier.class))
                .withIncludedTarget(MANAGEMENT_CENTER)
                .withExcludedTarget(MANAGEMENT_CENTER);

        assertTrue(original.isTargetExcluded(MANAGEMENT_CENTER));
        assertFalse(original.isTargetIncluded(MANAGEMENT_CENTER));
    }

    @Test
    public void testWithExcludedTargetsOverwrites() {
        MetricDescriptorImpl original = new MetricDescriptorImpl(mock(Supplier.class))
                .withExcludedTarget(JMX)
                .withExcludedTargets(MetricTarget.asSet(MANAGEMENT_CENTER, DIAGNOSTICS));

        assertTrue(original.isTargetExcluded(MANAGEMENT_CENTER));
        assertTrue(original.isTargetExcluded(DIAGNOSTICS));
        assertTrue(original.isTargetIncluded(JMX));
    }
}
