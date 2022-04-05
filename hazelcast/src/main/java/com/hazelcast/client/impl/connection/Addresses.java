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

package com.hazelcast.client.impl.connection;

import com.hazelcast.cluster.Address;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * A collection of addresses. It is split in a group of primary
 * addresses (so the ones that should be tried first) and a group
 * of secondary addresses (addresses that should be tried when the
 * primary group of addresses could not be connected to).
 */
public class Addresses {
    private final List<Address> primary = new LinkedList<Address>();
    private final List<Address> secondary = new LinkedList<Address>();

    public Addresses() {
    }

    public Addresses(Collection<Address> primary) {
        this.primary.addAll(primary);
    }

    public void addAll(Addresses addresses) {
        primary.addAll(addresses.primary);
        secondary.addAll(addresses.secondary);
    }

    public List<Address> primary() {
        return primary;
    }

    public List<Address> secondary() {
        return secondary;
    }
}
