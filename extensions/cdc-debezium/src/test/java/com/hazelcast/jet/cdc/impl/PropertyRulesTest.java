/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.jet.cdc.impl;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(QuickTest.class)
public class PropertyRulesTest {

    @Test
    public void requiredCheck() {
        PropertyRules rules = new PropertyRules().required("abc");
        Properties properties = new Properties();

        assertFail(rules, properties, "abc must be specified");

        properties.setProperty("abc", "test");
        rules.check(properties);
    }

    @Test
    public void requiredExcludedIncludedCombinedCheck() {
        PropertyRules rules = new PropertyRules()
                .required("abc")
                .exclusive("exclusive", "other")
                .inclusive("inclusive", "totallyOther");
        Properties properties = new Properties();
        properties.setProperty("exclusive", "test");
        properties.setProperty("other", "test");
        properties.setProperty("inclusive", "test");

        assertFail(rules, properties, "abc must be specified, exclusive and other are mutually exclusive, " +
                "inclusive requires totallyOther to be set too");
    }

    private void assertFail(PropertyRules rules, Properties properties, String messageExpected) {
        try {
            rules.check(properties);
            fail("IllegalStateException expected for rules " + rules + " and properties " + properties);
        } catch (IllegalStateException expected) {
            assertEquals(messageExpected, expected.getMessage());
        }
    }

}
