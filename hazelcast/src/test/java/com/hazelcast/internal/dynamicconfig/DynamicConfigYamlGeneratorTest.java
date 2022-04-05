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
import com.hazelcast.config.InMemoryYamlConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigYamlGeneratorTest extends AbstractDynamicConfigGeneratorTest {

    private static final ILogger LOGGER = Logger.getLogger(DynamicConfigYamlGeneratorTest.class);

    // LICENSE KEY

    @Test
    public void testLicenseKey() {
        String licenseKey = randomString();
        Config config = new Config().setLicenseKey(licenseKey);

        Config decConfig = getNewConfigViaGenerator(config);

        String actualLicenseKey = decConfig.getLicenseKey();
        assertEquals(config.getLicenseKey(), actualLicenseKey);
    }

    @Override
    protected Config getNewConfigViaGenerator(Config config) {
        DynamicConfigYamlGenerator dynamicConfigYamlGenerator = new DynamicConfigYamlGenerator();
        String yaml = dynamicConfigYamlGenerator.generate(config);
        LOGGER.fine("\n" + yaml);
        return new InMemoryYamlConfig(yaml);
    }
}
