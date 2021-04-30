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
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
public class YamlConfigSchemaValidatorTest {

    @After
    public void clearSysProp() {
        System.clearProperty("yaml.config.validation.skip");
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
        }
    }

    @Test
    public void causingExceptionsWrapped() {
        try {
            new YamlConfigBuilder(getClass().getResourceAsStream("/com/hazelcast/config/invalid-config.yaml")).build();
            fail("did not throw exception for invalid config");
        } catch (SchemaViolationConfigurationException e) {
            assertEquals("#/definitions/Map/additionalProperties", e.getKeywordLocation());
            assertEquals("#/hazelcast/map/my-map", e.getInstanceLocation());
            assertEquals(2, e.getErrors().size());
            Collection<String> expectedSubErrorMessages = asList(
                    "-1 is not greater or equal to 0", "#/definitions/BackupCount",
                    "expected type: Integer, found: String"
            );
            assertEquals(e.getErrors()
                    .stream().map(Exception::getMessage)
                    .filter(expectedSubErrorMessages::contains)
                    .count(), 2);
        }
    }

    @Test(expected = InvalidConfigurationException.class)
    public void emptyObject() {
        new YamlConfigBuilder(getClass().getResourceAsStream("/com/hazelcast/config/empty-object.yaml")).build();
    }
}
