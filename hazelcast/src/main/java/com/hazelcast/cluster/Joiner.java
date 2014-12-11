/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.nio.Address;

public interface Joiner {

    void join();

    void searchForOtherClusters();

    long getStartTime();

    void setTargetAddress(Address targetAddress);

    void reset();

    String getType();

    /**
     * Adds an address to the blacklist. Blacklist is useful if a node should ignore another node, e.g. when
     * the groupname of 2 machines is not the same and they should form different clusters.
     * <p/>
     * If blacklist is permanent, then this operation is write-once. It cannot be unblacklisted again.
     * If blacklist is temporary, blacklist can be removed via {@link #unblacklist(com.hazelcast.nio.Address)}.
     * <p/>
     * Method is thread-safe.
     * <p/>
     * If the address already is blacklisted, the call is ignored
     *
     * @param address   the address to blacklist.
     * @param permanent if blacklist is permanent or not
     * @throws java.lang.NullPointerException if address is null.
     * @see #isBlacklisted(com.hazelcast.nio.Address)
     */
    void blacklist(Address address, boolean permanent);

    /**
     * Removes an address from the blacklist if it's temporarily blacklisted.
     * This method has no effect if given address is not blacklisted. Permanent blacklists
     * cannot be undone.
     * <p/>
     * Method is thread-safe.
     * <p/>
     * If the address is not blacklisted, the call is ignored.
     *
     * @param address the address to unblacklist.
     * @return true if address is unblacklisted, false otherwise.
     */
    boolean unblacklist(Address address);

    /**
     * Checks if an address is blacklisted.
     *
     * Method is thread-safe.
     *
     * @param address the address to check.
     * @return true if blacklisted, false otherwise.
     * @throws java.lang.NullPointerException if address is null.
     * @see #blacklist(com.hazelcast.nio.Address, boolean)
     */
    boolean isBlacklisted(Address address);
}
