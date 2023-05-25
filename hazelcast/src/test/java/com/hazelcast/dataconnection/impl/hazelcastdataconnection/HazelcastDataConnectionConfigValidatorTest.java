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

package com.hazelcast.dataconnection.impl.hazelcastdataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.HazelcastDataConnection;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HazelcastDataConnectionConfigValidatorTest {

    @Test
    public void testValidateNone() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateEitherStringOrFilePath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML_PATH, "xml_path");
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, "xml");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateBothFilePathsSet() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML_PATH, "xml_path");
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_YML_PATH, "yaml_path");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateBothStringsSet() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, "xml");
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_YML, "yaml");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateOnlyFilePath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML_PATH, "xml_path");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateEmptyFilePath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML_PATH, "");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateOnlyString() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, "xml");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateEmptyString() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();
        dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, "");

        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();

        assertThatCode(() -> validator.validate(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }
}
