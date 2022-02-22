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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.dynamicconfig.DynamicConfigXmlGenerator.licenseKeyXmlGenerator;
import static org.junit.Assert.assertEquals;

// Please also take a look at the ConfigXmlGeneratorTest.
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigXmlGeneratorTest extends AbstractDynamicConfigGeneratorTest {

    private static final ILogger LOGGER = Logger.getLogger(DynamicConfigXmlGeneratorTest.class);

    // LICENSE KEY

    @Test
    public void testLicenseKey() {
        String licenseKey = randomString();
        Config config = new Config().setLicenseKey(licenseKey);

        StringBuilder xmlBuilder = new StringBuilder();
        ConfigXmlGenerator.XmlGenerator gen = new ConfigXmlGenerator.XmlGenerator(xmlBuilder);

        // generate string
        xmlBuilder.append("<hazelcast ")
                .append("xmlns=\"http://www.hazelcast.com/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"http://www.hazelcast.com/schema/config ")
                .append("http://www.hazelcast.com/schema/config/hazelcast-config-")
                .append(Versions.CURRENT_CLUSTER_VERSION.toString())
                .append(".xsd\">");
        licenseKeyXmlGenerator(gen, config);
        xmlBuilder.append("</hazelcast>");

        // recreate the config object
        String xmlString = xmlBuilder.toString();
        Config decConfig = new InMemoryXmlConfig(xmlString);

        // test
        String actualLicenseKey = decConfig.getLicenseKey();
        assertEquals(config.getLicenseKey(), actualLicenseKey);
    }

    @Override
    protected Config getNewConfigViaGenerator(Config config) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true, true);
        String xml = configXmlGenerator.generate(config);
        LOGGER.fine("\n" + xml);
        return new InMemoryXmlConfig(xml);

    }
}
