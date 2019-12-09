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

import com.hazelcast.query.impl.IndexDefinition;
import com.hazelcast.query.impl.IndexDefinition.UniqueKeyTransform;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BitmapIndexDefinitionParsingTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnMissingParenthesis() {
        IndexDefinition.parse("BITMAP(", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnEmpty() {
        IndexDefinition.parse("BITMAP()", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnTooManyParts() {
        IndexDefinition.parse("BITMAP(a, b, RAW, extra)", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnEmptyAttribute() {
        IndexDefinition.parse("BITMAP(, b)", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnEmptyKey() {
        IndexDefinition.parse("BITMAP(a,)", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnSameAttributeAndKey() {
        IndexDefinition.parse("BITMAP(a, a)", false);
    }

    @Test
    public void testValidDefinitions() {
        verify("BITMAP(a)", "a", "__key", UniqueKeyTransform.OBJECT);
        verify("BITMAP(y, z)", "y", "z", UniqueKeyTransform.OBJECT);
        verify("BITMAP(a, b, OBJECT)", "a", "b", UniqueKeyTransform.OBJECT);
        verify("BITMAP(a, b, LONG)", "a", "b", UniqueKeyTransform.LONG);
        verify("BITMAP(a, b, RAW)", "a", "b", UniqueKeyTransform.RAW);
        verify("BITMAP(this.a)", "a", "__key", UniqueKeyTransform.OBJECT);
        verify("BITMAP(this.a, __key#b, RAW)", "a", "__key.b", UniqueKeyTransform.RAW);
    }

    private static void verify(String text, String expectedAttribute, String expectedKey, UniqueKeyTransform expectedTransform) {
        IndexDefinition definition = IndexDefinition.parse(text, false);

        assertEquals(1, definition.getComponents().length);
        assertEquals(expectedAttribute, definition.getComponents()[0]);
        assertEquals(expectedKey, definition.getUniqueKey());
        assertEquals(expectedTransform, definition.getUniqueKeyTransform());
        assertEquals("BITMAP(" + expectedAttribute + ", " + expectedKey + ", " + expectedTransform + ")", definition.getName());
    }

}
