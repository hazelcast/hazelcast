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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.test.Accessors.getService;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigUpdateWithoutEnterpriseTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test(expected = UnsupportedOperationException.class)
    public void reloadWithoutEnterprise() throws IOException {
        String configAsString = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"></hazelcast>\n";
        File configFile = temporaryFolder.newFile(randomName() + ".xml");
        Files.write(configFile.toPath(), configAsString.getBytes());
        System.setProperty(SYSPROP_MEMBER_CONFIG, configFile.getAbsolutePath());

        ConfigurationService configurationService = getService(createHazelcastInstance(), ConfigurationService.SERVICE_NAME);
        configurationService.update();
    }
}
