/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class DynamicConfigSlowPreJoinBouncingTest extends DynamicConfigBouncingTest {

    public Config getConfig() {
        DelaysPreparingPreJoinOpService service = new DelaysPreparingPreJoinOpService();
        Config config = new Config();
        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true).setName(DelaysPreparingPreJoinOpService.SERVICE_NAME)
                        .setImplementation(service));
        return config;
    }

    private static class DelaysPreparingPreJoinOpService implements PreJoinAwareService {

        static final String SERVICE_NAME = "delaying-pre-join-op-prep-service";

        public DelaysPreparingPreJoinOpService() {
        }

        @Override
        public Operation getPreJoinOperation() {
            sleepSeconds(1);
            return null;
        }
    }
}
