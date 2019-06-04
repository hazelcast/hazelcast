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

package com.hazelcast.config;

import com.hazelcast.query.QueryConstants;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapAttributeConfigTest {

    @Test
    public void empty() {
        new MapAttributeConfig();
    }

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
    public void openingSquareBracketInName() {
        new MapAttributeConfig("co[m", "com.test.Extractor");
    }

    public void closingSquareBracketInName() {
        new MapAttributeConfig("co]m", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptySquareBracketInName() {
        new MapAttributeConfig("com[]", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void bothSquareBracketInName() {
        new MapAttributeConfig("com[007]", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void diactricsInName() {
        new MapAttributeConfig("ąćżźć^∆Ō∑ęĺłęŌ", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void spaceInName() {
        new MapAttributeConfig("cool attribute", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void queryConstantKeyAsName() {
        new MapAttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void queryConstantThisAsName() {
        new MapAttributeConfig(QueryConstants.THIS_ATTRIBUTE_NAME.value(), "com.test.Extractor");
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

    @Test
    public void validToString() {
        MapAttributeConfig config = new MapAttributeConfig("iq", "com.test.IqExtractor");

        String toString = config.toString();

        assertThat(toString, containsString("iq"));
        assertThat(toString, containsString("com.test.IqExtractor"));
    }

    @Test
    public void validReadOnly() {
        MapAttributeConfig config = new MapAttributeConfig("iq", "com.test.IqExtractor");

        MapAttributeConfigReadOnly readOnlyConfig = config.getAsReadOnly();

        assertThat(readOnlyConfig, instanceOf(MapAttributeConfigReadOnly.class));
        assertEquals("iq", readOnlyConfig.getName());
        assertEquals("com.test.IqExtractor", readOnlyConfig.getExtractor());
    }

}
