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

package com.hazelcast.config;

import com.hazelcast.internal.config.AttributeConfigReadOnly;
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
public class AttributeConfigTest {

    @Test
    public void empty() {
        new AttributeConfig();
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullName() {
        new AttributeConfig(null, "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyName() {
        new AttributeConfig("", "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void dotInName() {
        new AttributeConfig("body.brain", "com.class");
    }

    @Test(expected = IllegalArgumentException.class)
    public void openingSquareBracketInName() {
        new AttributeConfig("co[m", "com.test.Extractor");
    }

    public void closingSquareBracketInName() {
        new AttributeConfig("co]m", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptySquareBracketInName() {
        new AttributeConfig("com[]", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void bothSquareBracketInName() {
        new AttributeConfig("com[007]", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void diactricsInName() {
        new AttributeConfig("ąćżźć^∆Ō∑ęĺłęŌ", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void spaceInName() {
        new AttributeConfig("cool attribute", "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void queryConstantKeyAsName() {
        new AttributeConfig(QueryConstants.KEY_ATTRIBUTE_NAME.value(), "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void queryConstantThisAsName() {
        new AttributeConfig(QueryConstants.THIS_ATTRIBUTE_NAME.value(), "com.test.Extractor");
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullExtractor() {
        new AttributeConfig("iq", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyExtractor() {
        new AttributeConfig("iq", "");
    }

    @Test
    public void validDefinition() {
        AttributeConfig config = new AttributeConfig("iq", "com.test.IqExtractor");

        assertEquals("iq", config.getName());
        assertEquals("com.test.IqExtractor", config.getExtractorClassName());
    }

    @Test
    public void validToString() {
        AttributeConfig config = new AttributeConfig("iq", "com.test.IqExtractor");

        String toString = config.toString();

        assertThat(toString, containsString("iq"));
        assertThat(toString, containsString("com.test.IqExtractor"));
    }

    @Test
    public void validReadOnly() {
        AttributeConfig config = new AttributeConfig("iq", "com.test.IqExtractor");

        AttributeConfigReadOnly readOnlyConfig = new AttributeConfigReadOnly(config);

        assertThat(readOnlyConfig, instanceOf(AttributeConfigReadOnly.class));
        assertEquals("iq", readOnlyConfig.getName());
        assertEquals("com.test.IqExtractor", readOnlyConfig.getExtractorClassName());
    }

}
