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

import com.hazelcast.internal.yaml.YamlDomBuilder;
import com.hazelcast.internal.yaml.YamlMapping;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class YamlConfigValidatorTest {

    @After
    public void clearSysProp() {
        System.setProperty("yaml.config.validation.skip", "");
    }

    @Test
    public void validationDisabled() {
        System.setProperty("yaml.config.validation.skip", "true");
        YamlMapping config = (YamlMapping) YamlDomBuilder.build(new HashMap<>());
        YamlConfigSchemaValidator validator = YamlConfigSchemaValidator.create();
        validator.validate(config);
    }
    
    @Test
    public void validationExceptionIsWrapped() {
        YamlMapping config = (YamlMapping) YamlDomBuilder.build(new HashMap<>());
        YamlConfigSchemaValidator validator = YamlConfigSchemaValidator.create();
        try {
            validator.validate(config);
            fail("did not throw exception for invalid config");
        } catch (SchemaViolationConfigurationException e) {
            assertEquals("#", e.getKeywordLocation());
            assertEquals("#", e.getInstanceLocation());
            assertEquals("required key [hazelcast] not found", e.getMessage());
            assertEquals("required key [hazelcast] not found", e.getError());
        }
    }
}
