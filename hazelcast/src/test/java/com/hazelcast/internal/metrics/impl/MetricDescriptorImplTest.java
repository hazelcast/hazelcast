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

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import java.util.EnumSet;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
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
    public void testToString_emptyWithExcludedTargets() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier);

        assertEquals("[excludedTargets={}]",
                descriptor.toString());
    }

    @Test
    public void testToString_emptyWithoutExcludedTargets() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier);

        assertEquals("[]",
                descriptor.metricString());
    }

    @Test
    public void testMetricName_withPrefixAndDiscriminator() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withMetric("metricValue");

        assertEquals("[discriminatorTag=discriminatorValue,metric=prefix.metricValue]", descriptor.metricString());
    }

    @Test
    public void testMetricName_withPrefixWithoutDiscriminator() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withUnit(ProbeUnit.PERCENT);

        assertEquals("[unit=percent,metric=prefix.metricValue]", descriptor.metricString());
    }

    @Test
    public void testMetricName_withoutPrefixWithDiscriminator() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withMetric("metricValue");

        assertEquals("[discriminatorTag=discriminatorValue,metric=metricValue]", descriptor.metricString());
    }

    @Test
    public void testMetricName_withoutPrefixAndDiscriminator() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withMetric("metricValue");

        assertEquals("[metric=metricValue]", descriptor.metricString());
    }

    @Test
    public void testMetricName_withoutPrefixAndDiscriminator_withTags() {
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
                + "tag5=tag5Value]", descriptor.metricString());
    }

    @Test
    public void testMetricName_withPrefixAndDiscriminator_withTags() {
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
                        + "tag3=tag3Value,tag4=tag4Value,tag5=tag5Value]", descriptor.metricString());
    }

    @Test
    public void testMetricName_withPrefixAndDiscriminator_withSingleTag() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withPrefix("prefix")
                .withMetric("metricValue")
                .withDiscriminator("discriminatorTag", "discriminatorValue")
                .withUnit(MS)
                .withTag("tag0", "tag0Value");

        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metricValue,tag0=tag0Value]",
                descriptor.metricString());
    }

    @Test
    public void testCopy_withMetricName_withPrefixAndDiscriminator_withTags() {
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
                + "tag2=tag2Value]", descriptor.metricString());
        assertEquals("[discriminatorTag=discriminatorValue,unit=ms,metric=prefix.metric2,tag0=tag0Value,tag1=tag1Value,"
                + "tag2=tag2Value,tag3=tag3Value,tag4=tag4Value,tag5=tag5Value]", descriptorCopy.metricString());
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
                descriptor.metricString());
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
    public void testToString_includesEmptyExcludedTargets() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplier)
                .withTag("tag0", "tag0Value");

        assertEquals("[tag0=tag0Value,excludedTargets={}]",
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
    public void testEqualsDifferentTagsSameValues() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value");

        metricDescriptor2
                .withMetric("metricName")
                .withTag("tag0", "tag1Value")
                .withTag("tag1", "tag0Value");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsReversedTagAndValue() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value");

        metricDescriptor2
                .withMetric("metricName")
                .withTag("tag0Value", "tag0")
                .withTag("tag1Value", "tag1");

        assertNotEquals(metricDescriptor1, metricDescriptor2);
        assertNotEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
    }

    @Test
    public void testEqualsDifferentTagOrder() {
        MetricDescriptorImpl metricDescriptor1 = new MetricDescriptorImpl(mock(Supplier.class));
        MetricDescriptorImpl metricDescriptor2 = new MetricDescriptorImpl(mock(Supplier.class));

        metricDescriptor1
                .withMetric("metricName")
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value");

        metricDescriptor2
                .withMetric("metricName")
                .withTag("tag1", "tag1Value")
                .withTag("tag0", "tag0Value");

        assertEquals(metricDescriptor1, metricDescriptor2);
        assertEquals(metricDescriptor1.hashCode(), metricDescriptor2.hashCode());
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

    @Test
    public void testTagsCanGrow() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(new DefaultMetricDescriptorSupplier());
        for (int i = 0; i < 64; i++) {
            descriptor.withTag("tag" + i, "tag" + i + "Value");
            assertEquals("tag" + i + "Value", descriptor.tagValue("tag" + i));
            assertEquals(i + 1, descriptor.tagCount());
        }
        assertNull(descriptor.tagValue("unknownTag"));
    }

    @Test
    public void testCopiedDescriptorTagsCanGrow() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(new DefaultMetricDescriptorSupplier());
        for (int i = 0; i < 64; i++) {
            descriptor = descriptor.copy().withTag("tag" + i, "tag" + i + "Value");

            for (int j = 0; j <= i; j++) {
                assertEquals("tag" + j + "Value", descriptor.tagValue("tag" + j));
            }
            assertEquals(i + 1, descriptor.tagCount());
        }
        assertNull(descriptor.tagValue("unknownTag"));
    }

    @Test
    public void testCopiedIntoDescriptorTagsCanGrow() {
        Supplier supplierMock = mock(Supplier.class);
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(supplierMock);
        for (int i = 0; i < 64; i++) {
            String tag = "tag" + i;
            String tagValue = "tag" + i + "Value";
            descriptor = new MetricDescriptorImpl(supplierMock)
                    .copy(descriptor)
                    .withTag(tag, tagValue);

            for (int j = 0; j <= i; j++) {
                assertEquals("tag" + j + "Value", descriptor.tagValue("tag" + j));
            }
            assertEquals(tag, descriptor.tag(i));
            assertEquals(tagValue, descriptor.tagValue(i));
            assertEquals(i + 1, descriptor.tagCount());
        }
        assertNull(descriptor.tagValue("unknownTag"));
    }

    @Test
    public void testTagsWithNegativeIndexFails() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(mock(Supplier.class));
        try {
            assertNull(descriptor.tag(-1));
            fail("should have failed");
        } catch (IndexOutOfBoundsException expected) { }

        try {
            assertNull(descriptor.tagValue(-1));
            fail("should have failed");
        } catch (IndexOutOfBoundsException expected) { }
    }

    @Test
    public void testTagsWithTooHighIndexReturnsNull() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(mock(Supplier.class));
        try {
            assertNull(descriptor.tag(MAX_VALUE));
            fail("should have failed");
        } catch (IndexOutOfBoundsException expected) { }

        try {
            assertNull(descriptor.tagValue(MAX_VALUE));
            fail("should have failed");
        } catch (IndexOutOfBoundsException expected) { }
    }

    @Test
    public void testReadTags() {
        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(mock(Supplier.class))
                .withTag("tag0", "tag0Value")
                .withTag("tag1", "tag1Value")
                .withTag("tag2", "tag2Value")
                .withTag("tag3", "tag3Value")
                .withTag("tag4", "tag4Value");
        BiConsumer<String, String> tagConsumerMock = mock(BiConsumer.class);
        descriptor.readTags(tagConsumerMock);

        InOrder inOrder = inOrder(tagConsumerMock);
        inOrder.verify(tagConsumerMock).accept("tag0", "tag0Value");
        inOrder.verify(tagConsumerMock).accept("tag1", "tag1Value");
        inOrder.verify(tagConsumerMock).accept("tag2", "tag2Value");
        inOrder.verify(tagConsumerMock).accept("tag3", "tag3Value");
        inOrder.verify(tagConsumerMock).accept("tag4", "tag4Value");
        inOrder.verifyNoMoreInteractions();
    }
}
