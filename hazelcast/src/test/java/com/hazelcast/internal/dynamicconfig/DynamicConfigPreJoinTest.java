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
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.dynamicconfig.DynamicConfigBouncingTest.createMapConfig;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigPreJoinTest extends HazelcastTestSupport {

    private static final CountDownLatch holdSlaveStart = new CountDownLatch(1);
    private static final CountDownLatch canKillMaster = new CountDownLatch(1);

    private static final ILogger logger = Logger.getLogger(DynamicConfigPreJoinTest.class);

    @Test
    public void testDynamicConfigIsAddedToTheNewMemberDuringPreJoin() throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = smallInstanceConfig();
        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true)
                .setName(CountDownPreJoinAwareService.SERVICE_NAME)
                .setImplementation(new CountDownPreJoinAwareService());
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);

        HazelcastInstance master = factory.newHazelcastInstance(config);
        MapConfig mapConfig = createMapConfig(randomMapName());
        master.getConfig().addMapConfig(mapConfig);

        Future<HazelcastInstance> spawn = spawn(() -> {
            // factory.newHazelcastInstance(config) won't return before
            // holdSlaveStart.countDown()
            HazelcastInstance newSlave = factory.newHazelcastInstance(smallInstanceConfig());
            assertThat(newSlave.getConfig().getMapConfigs().values()).contains(mapConfig);
        });

        canKillMaster.await();
        master.getLifecycleService().terminate();

        holdSlaveStart.countDown();
        spawn.get();
    }

    private static class CountDownPreJoinAwareService implements PreJoinAwareService {
        static final String SERVICE_NAME = "count-down-pre-join-aware-service";

        @Override
        public Operation getPreJoinOperation() {
            return new CountDownOperation();
        }
    }

    private static class CountDownOperation extends Operation {
        @Override
        public void run() throws Exception {
            canKillMaster.countDown();
            logger.info("Before await");
            // You should see [127.0.0.1]:5701 is SHUTDOWN log between before and
            // after awaits. This is because we count down the canKillMaster latch
            // here.
            holdSlaveStart.await();
            logger.info("After await");
            throw new Exception("Expected Exception");
        }
    }
}
