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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalAddressRegistryTest extends HazelcastTestSupport {

    private final ILogger logger = Logger.getLogger(LocalAddressRegistry.class);
    private LocalAddressRegistry addressRegistry;

    @Before
    public void setUp() {
        addressRegistry = new LocalAddressRegistry(logger);
    }

    @Test
    public void whenRegistrationCountEqualsToRemovalCount_addressesShouldBeRemoved() throws UnknownHostException {
        UUID memberUuid = UuidUtil.newUnsecureUUID();
        addressRegistry.register(memberUuid, sampleAddresses());
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses().getAllAddresses());
        addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses().getPrimaryAddress());
        assertUUID_And_AddressesRemoved(memberUuid, sampleAddresses().getAllAddresses());
    }

    @Test
    public void whenRegistrationCountEqualsToRemovalCount_addressesShouldBeRemoved2() throws UnknownHostException {
        int count = 100;
        UUID memberUuid = UuidUtil.newUnsecureUUID();
        for (int i = 0; i < count; i++) {
            addressRegistry.register(memberUuid, sampleAddresses());
        }
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses().getAllAddresses());
        for (int i = 0; i < count; i++) {
            addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses().getPrimaryAddress());
        }
        assertUUID_And_AddressesRemoved(memberUuid, sampleAddresses().getAllAddresses());
    }

    @Test
    public void whenRegistrationCountNotEqualToRemovalCount_addressesShouldBeRemoved2() throws UnknownHostException {
        int count = 100;
        UUID memberUuid = UuidUtil.newUnsecureUUID();
        for (int i = 0; i < count; i++) {
            addressRegistry.register(memberUuid, sampleAddresses());
        }
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses().getAllAddresses());
        for (int i = 0; i < count - 1; i++) {
            addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses().getPrimaryAddress());
        }
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses().getAllAddresses());
    }

    @Test
    public void whenDisjointAddressSetRegisteredToSameUUID_oldAddressesShouldBeRemoved() throws UnknownHostException {
        UUID memberUuid = UuidUtil.newUnsecureUUID();
        addressRegistry.register(memberUuid, sampleAddresses());
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses().getAllAddresses());
        // disjoint address set is registered to the same uuid
        addressRegistry.register(memberUuid, sampleAddresses2());
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses2().getAllAddresses());
        assertAddressesRemoved(memberUuid, sampleAddresses().getAllAddresses());

        // remove registration attempts with the old registered addresses should fail.
        // try to remove twice in case we can't remove it because of the registration count
        addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses().getPrimaryAddress());
        addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses().getPrimaryAddress());
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses2().getAllAddresses());

        // try removal with the last addresses
        addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses2().getPrimaryAddress());
        assertUUID_And_AddressesRemoved(memberUuid, sampleAddresses2().getAllAddresses());
    }

    @Test
    public void whenDisjointAddressSetRegisteredToSameUUID_oldAddressesShouldBeRemoved2() throws UnknownHostException {
        int registerCount = 100;
        UUID memberUuid = UuidUtil.newUnsecureUUID();
        for (int i = 0; i < registerCount; i++) {
            addressRegistry.register(memberUuid, sampleAddresses());
        }
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses().getAllAddresses());

        // disjoint address set is registered to the same uuid
        for (int i = 0; i < registerCount; i++) {
            addressRegistry.register(memberUuid, sampleAddresses2());
        }
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses2().getAllAddresses());
        assertAddressesRemoved(memberUuid, sampleAddresses().getAllAddresses());

        // remove registration attempts with the old registered addresses should fail.
        // try to remove 2x times in case we can't remove it because of the registration count
        for (int i = 0; i < 2 * registerCount; i++) {
            addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses().getPrimaryAddress());
        }
        // show that removal requests on stale items do not remove the newly registered entries
        assertExpectedAddressesRegistered(memberUuid, sampleAddresses2().getAllAddresses());

        // try removal with the second set of addresses
        for (int i = 0; i < registerCount; i++) {
            addressRegistry.tryRemoveRegistration(memberUuid, sampleAddresses2().getPrimaryAddress());
        }
        assertUUID_And_AddressesRemoved(memberUuid, sampleAddresses2().getAllAddresses());
    }


    @Test
    public void test_whenDisjointAddressSetRegisteredToSameUUID_and_oldAddressSetRegisteredToDifferentUuid()
            throws UnknownHostException {
        UUID firstMemberUuid = UuidUtil.newUnsecureUUID();
        UUID secondMemberUuid = UuidUtil.newUnsecureUUID();
        addressRegistry.register(firstMemberUuid, sampleAddresses());
        assertExpectedAddressesRegistered(firstMemberUuid, sampleAddresses().getAllAddresses());
        // same addresses with new uuid
        addressRegistry.register(secondMemberUuid, sampleAddresses());
        assertExpectedAddressesRegistered(secondMemberUuid, sampleAddresses().getAllAddresses());
        // old uuid with new addresses
        addressRegistry.register(firstMemberUuid, sampleAddresses2());
        assertExpectedAddressesRegistered(firstMemberUuid, sampleAddresses2().getAllAddresses());
        assertAddressesRemoved(firstMemberUuid, sampleAddresses().getAllAddresses());

        addressRegistry.tryRemoveRegistration(firstMemberUuid, sampleAddresses().getPrimaryAddress());
        addressRegistry.tryRemoveRegistration(firstMemberUuid, sampleAddresses().getPrimaryAddress());
        // show that removal requests on stale items do not remove the newly registered entries
        assertExpectedAddressesRegistered(firstMemberUuid, sampleAddresses2().getAllAddresses());

        addressRegistry.tryRemoveRegistration(firstMemberUuid, sampleAddresses2().getPrimaryAddress());
        addressRegistry.tryRemoveRegistration(secondMemberUuid, sampleAddresses().getPrimaryAddress());

        assertUUID_And_AddressesRemoved(firstMemberUuid, sampleAddresses2().getAllAddresses());
        assertUUID_And_AddressesRemoved(secondMemberUuid, sampleAddresses().getAllAddresses());
    }

    private void assertExpectedAddressesRegistered(UUID memberUuid, Set<Address> addresses) {
        for (Address address : addresses) {
            assertEquals(memberUuid, addressRegistry.uuidOf(address));
        }
        LinkedAddresses linkedAddresses = addressRegistry.linkedAddressesOf(memberUuid);
        assertNotNull(linkedAddresses);
        assertNotNull(linkedAddresses.getAllAddresses());
        assertTrue(linkedAddresses.getAllAddresses().containsAll(addresses));
    }

    private void assertAddressesRemoved(UUID memberUuid, Set<Address> removedAddresses) {
        for (Address address : removedAddresses) {
            UUID correspondingMemberUuid = addressRegistry.uuidOf(address);
            assertTrue(correspondingMemberUuid == null || correspondingMemberUuid != memberUuid);
        }
        LinkedAddresses linkedAddresses = addressRegistry.linkedAddressesOf(memberUuid);
        if (linkedAddresses != null) {
            for (Address address : removedAddresses) {
                assertNotContains(linkedAddresses.getAllAddresses(), address);
            }
        }
    }

    private void assertUUID_And_AddressesRemoved(UUID memberUuid, Set<Address> addresses) {
        for (Address address : addresses) {
            assertNull(addressRegistry.uuidOf(address));
        }
        LinkedAddresses linkedAddresses = addressRegistry.linkedAddressesOf(memberUuid);
        assertNull(linkedAddresses);
    }

    private static LinkedAddresses sampleAddresses() throws UnknownHostException {
        LinkedAddresses addresses = new LinkedAddresses(new Address("localhost", 5701));
        addresses.addAllResolvedAddresses(new Address("127.0.0.1", 5701));
        return addresses;
    }

    private static LinkedAddresses sampleAddresses2() throws UnknownHostException {
        LinkedAddresses addresses = new LinkedAddresses(new Address("localhost", 5702));
        addresses.addAllResolvedAddresses(new Address("127.0.0.1", 5702));
        return addresses;
    }
}
