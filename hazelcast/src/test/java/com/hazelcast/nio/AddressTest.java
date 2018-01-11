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

package com.hazelcast.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AddressTest {

    @Test(expected = IllegalArgumentException.class)
    public void newAddress_InetSocketAddress_whenHostUnresolved() throws UnknownHostException {
        InetSocketAddress inetAddress = InetSocketAddress.createUnresolved("dontexist", 1);
        new Address(inetAddress);
    }

    @Test(expected = NullPointerException.class)
    public void newAddress_InetSocketAddress_whenNull() throws UnknownHostException {
        new Address((InetSocketAddress) null);
    }
}
