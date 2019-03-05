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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.predicates.PredicateUtils.canonicalizeAttribute;
import static com.hazelcast.query.impl.predicates.PredicateUtils.parseOutCompositeIndexComponents;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AttributeCanonicalizationTest {

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyComponentIsNotAllowed() {
        parseOutCompositeIndexComponents("a,");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateComponentsAreNotAllowed() {
        parseOutCompositeIndexComponents("a,b,a");
    }

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
        Indexes indexes = Indexes.newBuilder(new DefaultSerializationServiceBuilder().build(), IndexCopyBehavior.NEVER).build();

        checkIndex(indexes, "foo", "foo");
        checkIndex(indexes, "foo", "this.foo");
        checkIndex(indexes, "this", "this");
        checkIndex(indexes, "foo.this.bar", "foo.this.bar");
        checkIndex(indexes, "foo.bar", "this.foo.bar");
        checkIndex(indexes, "foo.bar", "foo.bar");
        checkIndex(indexes, "__key", "__key");
        checkIndex(indexes, "__key.foo", "__key.foo");
    }

    @Test
    public void testCompositeIndexes() {
        Indexes indexes = Indexes.newBuilder(new DefaultSerializationServiceBuilder().build(), IndexCopyBehavior.NEVER).build();

        checkIndex(indexes, "foo, bar", "foo, bar");
        checkIndex(indexes, "foo, bar", "foo , bar");
        checkIndex(indexes, "foo, bar", "this.foo, bar");
        checkIndex(indexes, "this, __key", "this,__key");
        checkIndex(indexes, "foo, bar.this.baz", "foo, bar.this.baz");
        checkIndex(indexes, "foo, bar.baz", "this.foo, this.bar.baz");
        checkIndex(indexes, "foo.bar, baz", "foo.bar, baz");
        checkIndex(indexes, "foo, bar, __key.baz", "foo, this.bar, __key.baz");
    }

    private static void checkIndex(Indexes indexes, String expected, String name) {
        indexes.destroyIndexes();
        assertFalse(indexes.haveAtLeastOneIndex());

        InternalIndex index = indexes.addOrGetIndex(name, false);
        assertEquals(expected, index.getName());

        assertNotNull(indexes.getIndex(expected));
    }

    private static class TestPredicate extends AbstractPredicate {

        public TestPredicate(String attribute) {
            super(attribute);
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
            return false;
        }

    }

}
