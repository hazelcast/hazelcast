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

package com.hazelcast.query.impl;

import com.hazelcast.query.impl.AttributeIndexRegistry.FirstComponentDecorator;
import com.hazelcast.query.impl.QueryContext.IndexMatchHint;
import com.hazelcast.query.impl.predicates.PredicateUtils;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AttributeIndexRegistryTest {

    private AttributeIndexRegistry registry;

    @Before
    public void before() {
        registry = new AttributeIndexRegistry();
    }

    @Test
    public void testNonCompositeIndexes() {
        InternalIndex orderedA = index(true, "a");
        registry.register(orderedA);
        assertSame(orderedA, registry.match("a", IndexMatchHint.NONE));
        assertSame(orderedA, registry.match("a", IndexMatchHint.PREFER_ORDERED));
        assertSame(orderedA, registry.match("a", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        InternalIndex unorderedB = index(false, "b");
        registry.register(unorderedB);
        assertSame(orderedA, registry.match("a", IndexMatchHint.NONE));
        assertSame(orderedA, registry.match("a", IndexMatchHint.PREFER_ORDERED));
        assertSame(orderedA, registry.match("a", IndexMatchHint.PREFER_UNORDERED));
        assertSame(unorderedB, registry.match("b", IndexMatchHint.NONE));
        assertSame(unorderedB, registry.match("b", IndexMatchHint.PREFER_ORDERED));
        assertSame(unorderedB, registry.match("b", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        InternalIndex unorderedA = index(false, "a");
        registry.register(unorderedA);
        assertThat(registry.match("a", IndexMatchHint.NONE), anyOf(sameInstance(orderedA), sameInstance(unorderedA)));
        assertSame(orderedA, registry.match("a", IndexMatchHint.PREFER_ORDERED));
        assertSame(unorderedA, registry.match("a", IndexMatchHint.PREFER_UNORDERED));
        assertSame(unorderedB, registry.match("b", IndexMatchHint.NONE));
        assertSame(unorderedB, registry.match("b", IndexMatchHint.PREFER_ORDERED));
        assertSame(unorderedB, registry.match("b", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        registry.clear();
        assertNull(registry.match("a", IndexMatchHint.NONE));
        assertNull(registry.match("b", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));
    }

    @Test
    public void testCompositeIndexes() {
        InternalIndex orderedA12 = index(true, "a1", "a2");
        registry.register(orderedA12);
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.NONE)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_ORDERED)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_UNORDERED)));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        InternalIndex unorderedB12 = index(false, "b1", "b2");
        registry.register(unorderedB12);
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.NONE)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_ORDERED)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_UNORDERED)));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_ORDERED));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("b2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        InternalIndex unorderedA12 = index(false, "a1", "a2");
        registry.register(unorderedA12);
        assertThat(undecorated(registry.match("a1", IndexMatchHint.NONE)),
                anyOf(sameInstance(orderedA12), sameInstance(unorderedA12)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_ORDERED)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_UNORDERED)));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_ORDERED));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("b2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        registry.clear();
        assertNull(registry.match("a1", IndexMatchHint.NONE));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.NONE));
        assertNull(registry.match("b2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));
    }

    @Test
    public void testCompositeAndNonCompositeIndexes() {
        InternalIndex unorderedA1 = index(false, "a1");
        registry.register(unorderedA1);
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.NONE));
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.PREFER_ORDERED));
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        InternalIndex unorderedB12 = index(false, "b1", "b2");
        registry.register(unorderedB12);
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.NONE));
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.PREFER_ORDERED));
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_ORDERED));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("b2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        InternalIndex orderedA12 = index(true, "a1", "a2");
        registry.register(orderedA12);
        assertThat(undecorated(registry.match("a1", IndexMatchHint.NONE)),
                anyOf(sameInstance(unorderedA1), sameInstance(orderedA12)));
        assertSame(orderedA12, undecorated(registry.match("a1", IndexMatchHint.PREFER_ORDERED)));
        assertSame(unorderedA1, registry.match("a1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_ORDERED));
        assertNull(registry.match("b1", IndexMatchHint.PREFER_UNORDERED));
        assertNull(registry.match("b2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));

        registry.clear();
        assertNull(registry.match("a1", IndexMatchHint.NONE));
        assertNull(registry.match("a2", IndexMatchHint.NONE));
        assertNull(registry.match("b1", IndexMatchHint.NONE));
        assertNull(registry.match("b2", IndexMatchHint.NONE));
        assertNull(registry.match("unknown", IndexMatchHint.NONE));
    }

    @Test
    public void testNonCompositeIndexesArePreferredOverComposite() {
        InternalIndex a12 = index(true, "a1", "a2");
        registry.register(a12);
        assertSame(a12, undecorated(registry.match("a1", IndexMatchHint.NONE)));

        InternalIndex a1 = index(true, "a1");
        registry.register(a1);
        assertSame(a1, registry.match("a1", IndexMatchHint.NONE));
    }

    @Test
    public void testShorterCompositeIndexesArePreferredOverLonger() {
        InternalIndex a123 = index(true, "a1", "a2", "a3");
        registry.register(a123);
        assertSame(a123, undecorated(registry.match("a1", IndexMatchHint.NONE)));

        InternalIndex a12 = index(true, "a1", "a2");
        registry.register(a12);
        assertSame(a12, undecorated(registry.match("a1", IndexMatchHint.NONE)));
    }

    private static InternalIndex index(boolean ordered, String... components) {
        InternalIndex index = Mockito.mock(InternalIndex.class);
        if (components.length == 1) {
            when(index.getName()).thenReturn(components[0]);
            when(index.getComponents()).thenReturn(null);
        } else {
            when(index.getName()).thenReturn(PredicateUtils.constructCanonicalCompositeIndexName(components));
            when(index.getComponents()).thenReturn(components);
        }
        when(index.isOrdered()).thenReturn(ordered);
        return index;
    }

    private static InternalIndex undecorated(InternalIndex index) {
        return index instanceof FirstComponentDecorator ? ((FirstComponentDecorator) index).delegate : index;
    }

}
