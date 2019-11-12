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

package com.hazelcast.internal.metrics.jmx;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.internal.util.BiTuple.of;
import static com.hazelcast.internal.util.MapUtil.entry;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JmxPublisherTest {

    private static final String MODULE_NAME = "moduleA";

    private ObjectName objectNameNoModule;
    private ObjectName objectNameWithModule;

    private JmxPublisher jmxPublisher;
    private MBeanServer platformMBeanServer;
    private String domainPrefix;

    @Before
    public void before() throws Exception {
        domainPrefix = "domain" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        jmxPublisher = new JmxPublisher("inst1", domainPrefix);
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectNameNoModule = new ObjectName(domainPrefix + ":*");
        objectNameWithModule = new ObjectName(domainPrefix + "." + MODULE_NAME + ":*");
        try {
            assertMBeans(emptyList());
        } catch (AssertionError e) {
            throw new AssertionError("JMX beans used by this test are already registered", e);
        }
    }

    @After
    public void after() throws Exception {
        // unregister all beans in domains used in the test to not interfere with other tests
        try {
            for (ObjectInstance instance : queryOurInstances()) {
                platformMBeanServer.unregisterMBean(instance.getObjectName());
            }
        } catch (InstanceNotFoundException ignored) {
        }
    }

    @Test
    public void when_singleMetric() throws Exception {
        MetricDescriptor descriptor = newDescriptor()
                .withMetric("c")
                .withTag("tag1", "a")
                .withTag("tag2", "b");
        jmxPublisher.publishLong(descriptor, 1L);
        assertMBeans(singletonList(
                of(domainPrefix + ":type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=b\"",
                        singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_singleMetricWithPrefixAndDiscriminator() throws Exception {
        MetricDescriptor descriptor = newDescriptor()
                .withPrefix("d")
                .withMetric("c")
                .withDiscriminator("name", "itsName")
                .withTag("tag1", "a")
                .withTag("tag2", "b");
        jmxPublisher.publishLong(descriptor, 1L);
        assertMBeans(singletonList(
                of(domainPrefix + ":type=Metrics,instance=inst1,prefix=d,tag0=\"tag1=a\",tag1=\"tag2=b\",tag2=\"name=itsName\"",
                        singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_singleMetricWithModule() throws Exception {
        MetricDescriptor descriptor = newDescriptor()
                .withMetric("c")
                .withTag("tag1", "a")
                .withTag("module", MODULE_NAME);
        jmxPublisher.publishLong(descriptor, 1L);
        assertMBeans(singletonList(
                of(domainPrefix + "." + MODULE_NAME + ":type=Metrics,instance=inst1,tag0=\"tag1=a\"",
                        singletonList(entry("c", 1L)))));
    }

    @Test
    public void when_moreMetrics() throws Exception {
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("c")
                .withTag("tag1", "a")
                .withTag("tag2", "b"), 1L);
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("d")
                .withTag("tag1", "a")
                .withTag("tag2", "b"), 2L);
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("d")
                .withTag("module", MODULE_NAME)
                .withTag("tag1", "a")
                .withTag("tag2", "b"), 5L);
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("a")
                .withTag("tag1", "a")
                .withTag("tag2", "c"), 3L);
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("a"), 4L);
        assertMBeans(asList(
                of(domainPrefix + ":type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=b\"",
                        asList(entry("c", 1L), entry("d", 2L))),
                of(domainPrefix + "." + MODULE_NAME + ":type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=b\"",
                        singletonList(entry("d", 5L))),
                of(domainPrefix + ":type=Metrics,instance=inst1,tag0=\"tag1=a\",tag1=\"tag2=c\"",
                        singletonList(entry("a", 3L))),
                of(domainPrefix + ":type=Metrics,instance=inst1",
                        singletonList(entry("a", 4L)))
        ));
    }

    @Test
    public void when_metricNotRendered_then_mBeanRemoved() throws Exception {
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("b")
                .withTag("tag1", "a"), 1L);
        jmxPublisher.publishLong(newDescriptor()
                .withMetric("c")
                .withTag("tag1", "a"), 2L);
        jmxPublisher.whenComplete();
        assertMBeans(singletonList(
                of(domainPrefix + ":type=Metrics,instance=inst1,tag0=\"tag1=a\"",
                        asList(entry("b", 1L), entry("c", 2L)))
        ));

        jmxPublisher.publishLong(newDescriptor()
                .withMetric("b")
                .withTag("tag1", "a"), 1L);
        jmxPublisher.whenComplete();
        assertMBeans(singletonList(
                of(domainPrefix + ":type=Metrics,instance=inst1,tag0=\"tag1=a\"",
                        singletonList(entry("b", 1L)))
        ));

        jmxPublisher.whenComplete();
        assertMBeans(emptyList());
    }

    @Test
    public void when_badCharacters1_then_escaped() throws Exception {
        // this is a test that the test works with plain input
        when_badCharacters_then_escaped("aaa");
    }

    @Test
    public void when_badCharacters2_then_escaped() throws Exception {
        // backslash is special in quoted value, but not in unquoted
        when_badCharacters_then_escaped("\\");
    }

    @Test
    public void when_badCharacters3_then_escaped() throws Exception {
        // colon is special in unquoted value, but not in quoted
        when_badCharacters_then_escaped(":");
    }

    @Test
    public void when_badCharacters4_then_escaped() throws Exception {
        // a text containing characters that need to be escaped in quoted value
        when_badCharacters_then_escaped("\\w\n\\");
    }

    @Test
    public void when_badCharacters5_then_escaped() throws Exception {
        // * and ? can be validly a part of unquoted or quoted value, but they are pattern matchers
        // and we want to treat them literally
        when_badCharacters_then_escaped("?*");
    }

    private void when_badCharacters_then_escaped(String badText) throws Exception {
        // we must be able to work with any crazy user input
        System.out.println("badText: " + badText);
        jmxPublisher.publishLong(newDescriptor()
                .withPrefix(badText)
                .withMetric("metric"), 1L);
        jmxPublisher.publishLong(newDescriptor()
                .withMetric(badText)
                .withTag("tag1", "a"), 2L);
        jmxPublisher.whenComplete();

        assertMBeans(asList(
                of(domainPrefix + ":type=Metrics,instance=inst1,prefix=" + JmxPublisher.escapeObjectNameValue(badText),
                        singletonList(entry("metric", 1L))),
                of(domainPrefix + ":type=Metrics,instance=inst1,tag0=\"tag1=a\"",
                        singletonList(entry(badText, 2L)))
        ));
    }

    @Test
    public void when_jmxExcluded_notPublished() throws Exception {
        jmxPublisher.publishLong(newDescriptor()
                .withPrefix("excluded")
                .withMetric("long")
                .withExcludedTarget(JMX), 1L);
        jmxPublisher.publishLong(newDescriptor()
                .withPrefix("notExcluded")
                .withMetric("long"), 2L);
        jmxPublisher.publishDouble(newDescriptor()
                .withPrefix("excluded")
                .withMetric("double")
                .withExcludedTarget(JMX), 1.5D);
        jmxPublisher.publishDouble(newDescriptor()
                .withPrefix("notExcluded")
                .withMetric("double"), 2.5D);
        jmxPublisher.whenComplete();

        assertMBeans(singletonList(
                of(domainPrefix + ":type=Metrics,instance=inst1,prefix=notExcluded", asList(
                        entry("long", 2L),
                        entry("double", 2.5D)
                ))
        ));
    }

    private MetricDescriptor newDescriptor() {
        return DEFAULT_DESCRIPTOR_SUPPLIER.get();
    }

    private void assertMBeans(List<BiTuple<String, List<Entry<String, Number>>>> expected) throws Exception {
        Set<ObjectInstance> instances = queryOurInstances();
        Map<ObjectName, ObjectInstance> instanceMap =
                instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));

        assertEquals("actual: " + instances, expected.size(), instances.size());

        for (BiTuple<String, List<Entry<String, Number>>> entry : expected) {
            ObjectName on = new ObjectName(entry.element1);
            assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));
            for (Entry<String, Number> attribute : entry.element2) {
                assertEquals("Attribute '" + attribute.getKey() + "' of '" + on + "' doesn't match",
                        attribute.getValue(), platformMBeanServer.getAttribute(on, attribute.getKey()));
            }
        }
    }

    private Set<ObjectInstance> queryOurInstances() {
        Set<ObjectInstance> instances = new HashSet<>();
        instances.addAll(platformMBeanServer.queryMBeans(objectNameNoModule, null));
        instances.addAll(platformMBeanServer.queryMBeans(objectNameWithModule, null));
        return instances;
    }
}
