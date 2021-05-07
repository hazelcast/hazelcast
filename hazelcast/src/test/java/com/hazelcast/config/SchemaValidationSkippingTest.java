/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SchemaValidationSkippingTest {

    @Rule
    public OverridePropertyRule skipProp = OverridePropertyRule.clear("hazelcast.config.schema.validation.enabled");

    @Test
    public void testSkippingYamlSchemaValidation() {
        System.setProperty("hazelcast.config.schema.validation.enabled", "false");
        String yaml = "invalid: yes";
        YamlConfigBuilderTest.buildConfig(yaml);
    }

    @Test
    public void testSkippingXmlSchemaValidation() {
        System.setProperty("hazelcast.config.schema.validation.enabled", "false");
        String xml = HAZELCAST_START_TAG + "<invalid></invalid>" + HAZELCAST_END_TAG;
        XMLConfigBuilderTest.buildConfig(xml);
    }

}
