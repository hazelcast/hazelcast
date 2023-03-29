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

package com.hazelcast.datalink.impl.hazelcastdatalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.HazelcastDataLink;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HazelcastDataLinkConfigValidatorTest {

    @Test
    public void testValidateEitherStringOrFilePath() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML_PATH, "xml_path");
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML, "xml");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataLinkConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateBothFilePathsSet() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML_PATH, "xml_path");
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_YML_PATH, "yaml_path");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataLinkConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateBothStringsSet() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML, "xml");
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_YML, "yaml");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatThrownBy(() -> validator.validate(dataLinkConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateOnlyFilePath() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML_PATH, "xml_path");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatCode(() -> validator.validate(dataLinkConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateEmptyFilePath() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML_PATH, "");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatCode(() -> validator.validate(dataLinkConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void testValidateOnlyString() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML, "xml");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatCode(() -> validator.validate(dataLinkConfig))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidateEmptyString() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig();
        dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML, "");

        HazelcastDataLinkConfigValidator validator = new HazelcastDataLinkConfigValidator();

        assertThatCode(() -> validator.validate(dataLinkConfig))
                .isInstanceOf(HazelcastException.class);
    }
}
