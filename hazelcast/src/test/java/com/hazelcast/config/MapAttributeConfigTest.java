/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.query.QueryConstants;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapAttributeConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void nullName() {
        new MapAttributeConfig(null, "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyName() {
        new MapAttributeConfig("", "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void dotInName() {
        new MapAttributeConfig("body.brain", "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void queryConstantAsName() {
        new MapAttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void openingSquareBracketInName() {
        new MapAttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "co[m");
    }

    public void closingSquareBracketInName() {
        new MapAttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "co]m");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptySquareBracketInName() {
        new MapAttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "com[]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void bothSquareBracketInName() {
        new MapAttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "com[007]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullExtractor() {
        new MapAttributeConfig("iq", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyExtractor() {
        new MapAttributeConfig("iq", "");
    }

    @Test
    public void validDefinition() {
        MapAttributeConfig config = new MapAttributeConfig("iq", "com.test.IqExtractor");

        assertEquals("iq", config.getName());
        assertEquals("com.test.IqExtractor", config.getExtractor());
    }

}
