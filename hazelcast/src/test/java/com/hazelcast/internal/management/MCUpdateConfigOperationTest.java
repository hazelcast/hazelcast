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

package com.hazelcast.internal.management;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.codec.MCUpdateConfigCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.map.AbstractClientMapTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

public class MCUpdateConfigOperationTest
        extends AbstractClientMapTest {

    @Test
    public void test()
            throws ExecutionException, InterruptedException {
        ClientInvocation inv = new ClientInvocation(
                ((HazelcastClientProxy) client).client,
                MCUpdateConfigCodec.encodeRequest(
                        "hazelcast:\n"
                                + "  map:\n"
                                + "    added-map:\n"
                                + "      backup-count: 3\n"
                ),
                null
        );
        try {
            inv.invoke().get();
            fail("did not throw exception");
        } catch (ExecutionException e) {
            assertInstanceOf(UnsupportedOperationException.class, e.getCause());
        }
    }
}
