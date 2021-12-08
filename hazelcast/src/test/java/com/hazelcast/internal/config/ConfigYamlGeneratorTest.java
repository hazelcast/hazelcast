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

package com.hazelcast.internal.config;

import com.hazelcast.config.AbstractConfigGeneratorTest;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryYamlConfig;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigYamlGeneratorTest extends AbstractConfigGeneratorTest {

    @Override
    protected Config getNewConfigViaGenerator(Config config) {
        ConfigYamlGenerator configYamlGenerator = new ConfigYamlGenerator();
        String yaml = configYamlGenerator.generate(config);
        return new InMemoryYamlConfig(yaml);
    }
}
