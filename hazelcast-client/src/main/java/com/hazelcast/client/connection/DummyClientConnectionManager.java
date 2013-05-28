/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.nio.Address;

import java.io.IOException;

/**
 * @ali 5/27/13
 */
public class DummyClientConnectionManager extends SmartClientConnectionManager {

    private volatile Address address;

    public DummyClientConnectionManager(HazelcastClient client, Authenticator authenticator, LoadBalancer loadBalancer) {
        super(client, authenticator, loadBalancer);
    }

    public Connection firstConnection(Address address, Authenticator authenticator) throws IOException {
        this.address = address;
        return newConnection(address, authenticator);
    }

    /**
     * get or create connection
     * @param address
     * @return
     * @throws IOException
     */
    public Connection getConnection(Address address) throws IOException {
        if (this.address != null){
            return super.getConnection(this.address);
        } else {
            return super.getConnection(address);
        }
    }


}
