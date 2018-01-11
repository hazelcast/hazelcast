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

package com.hazelcast.spring;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.MemberAddressProvider;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class DummyMemberAddressProvider implements MemberAddressProvider {

    private final Properties properties;

    public DummyMemberAddressProvider(Properties properties) {
        this.properties = properties;
    }

    @Override
    public InetSocketAddress getBindAddress() {
        try {
            return new Address("localhost", 1234).getInetSocketAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InetSocketAddress getPublicAddress() {
        try {
            return new Address("localhost", 1234).getInetSocketAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
