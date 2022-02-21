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

package com.hazelcast.client;

import com.hazelcast.client.impl.ClientExtension;
import com.hazelcast.client.impl.clientside.DefaultClientExtension;
import com.hazelcast.client.impl.spi.ClientProxyFactory;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExtensionTest extends HazelcastTestSupport {

    @Test
    public void test_createServiceProxyFactory() throws Exception {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertInstanceOf(ClientProxyFactory.class, clientExtension.createServiceProxyFactory(MapService.class));
    }

    @Test
    public void test_createServiceProxyFactory_whenUnknownServicePassed() {
        ClientExtension clientExtension = new DefaultClientExtension();
        Assertions.assertThatThrownBy(() -> clientExtension.createServiceProxyFactory(TestService.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Proxy factory cannot be created. Unknown service");
    }

    private static class TestService {

    }
}
