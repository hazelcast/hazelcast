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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.query.impl.IndexUtils.canonicalizeAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AttributeCanonicalizationTest {

    @Test
    public void testAttributes() {
        assertEquals("foo", canonicalizeAttribute("foo"));
        assertEquals("foo", canonicalizeAttribute("this.foo"));
        assertEquals("this", canonicalizeAttribute("this"));
        assertEquals("foo.this.bar", canonicalizeAttribute("foo.this.bar"));
        assertEquals("foo.bar", canonicalizeAttribute("this.foo.bar"));
        assertEquals("foo.bar", canonicalizeAttribute("foo.bar"));
        assertEquals("__key", canonicalizeAttribute("__key"));
        assertEquals("__key.foo", canonicalizeAttribute("__key.foo"));
    }

    @Test
    public void testAbstractPredicate() {
        assertEquals("foo", new TestPredicate("foo").attributeName);
        assertEquals("foo", new TestPredicate("this.foo").attributeName);
        assertEquals("this", new TestPredicate("this").attributeName);
        assertEquals("foo.this.bar", new TestPredicate("foo.this.bar").attributeName);
        assertEquals("foo.bar", new TestPredicate("this.foo.bar").attributeName);
        assertEquals("foo.bar", new TestPredicate("foo.bar").attributeName);
        assertEquals("__key", new TestPredicate("__key").attributeName);
        assertEquals("__key.foo", new TestPredicate("__key.foo").attributeName);
    }

    @Test
    public void testIndexes() {
        Indexes indexes = Indexes.newBuilder(
                new DefaultSerializationServiceBuilder().build(), IndexCopyBehavior.NEVER, DEFAULT_IN_MEMORY_FORMAT).build();

        checkIndex(indexes, "foo", "foo");
        checkIndex(indexes, "this.foo", "foo");
        checkIndex(indexes, "this", "this");
        checkIndex(indexes, "foo.this.bar", "foo.this.bar");
        checkIndex(indexes, "this.foo.bar", "foo.bar");
        checkIndex(indexes, "foo.bar", "foo.bar");
        checkIndex(indexes, "__key", "__key");
        checkIndex(indexes, "__key.foo", "__key.foo");
    }

    @Test
    public void testCompositeIndexes() {
        Indexes indexes = Indexes.newBuilder(
                new DefaultSerializationServiceBuilder().build(), IndexCopyBehavior.NEVER, DEFAULT_IN_MEMORY_FORMAT).build();

        checkIndex(indexes, new String[]{"foo", "bar"}, new String[]{"foo", "bar"});
        checkIndex(indexes, new String[]{"this.foo", "bar"}, new String[]{"foo", "bar"});
        checkIndex(indexes, new String[]{"this", "__key"}, new String[]{"this", "__key"});
        checkIndex(indexes, new String[]{"foo", "bar.this.baz"}, new String[]{"foo", "bar.this.baz"});
        checkIndex(indexes, new String[]{"this.foo", "bar.this.baz"}, new String[]{"foo", "bar.this.baz"});
        checkIndex(indexes, new String[]{"foo.bar", "baz"}, new String[]{"foo.bar", "baz"});
        checkIndex(indexes, new String[]{"foo", "this.bar", "__key.baz"}, new String[]{"foo", "bar", "__key.baz"});
    }

    private void checkIndex(Indexes indexes, String attribute, String expAttribute) {
        checkIndex(indexes, new String[]{attribute}, new String[]{expAttribute});
    }

    private void checkIndex(Indexes indexes, String[] attributes, String[] expAttributes) {
        checkIndex(indexes, IndexType.HASH, attributes, expAttributes);
        checkIndex(indexes, IndexType.SORTED, attributes, expAttributes);
    }

    private void checkIndex(Indexes indexes, IndexType indexType, String[] attributes, String[] expAttributes) {
        IndexConfig config = new IndexConfig().setType(indexType);

        for (String attribute : attributes) {
            config.addAttribute(attribute);
        }

        IndexConfig normalizedConfig = IndexUtils.validateAndNormalize("map", config);

        StringBuilder expName = new StringBuilder("map");

        if (indexType == IndexType.SORTED) {
            expName.append("_sorted");
        } else {
            expName.append("_hash");
        }

        for (int i = 0; i < expAttributes.length; i++) {
            String expAttribute = expAttributes[i];
            String attribute = normalizedConfig.getAttributes().get(i);

            assertEquals(expAttribute, attribute);

            expName.append("_").append(expAttribute);
        }

        assertEquals(expName.toString(), normalizedConfig.getName());

        InternalIndex index = indexes.addOrGetIndex(normalizedConfig);
        assertEquals(normalizedConfig.getName(), index.getName());

        assertNotNull(indexes.getIndex(normalizedConfig.getName()));
    }

    private static class TestPredicate extends AbstractPredicate {

        TestPredicate(String attribute) {
            super(attribute);
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
            return false;
        }

    }

}
