/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableCollection;

/**
 * A special, immutable {@link MemberImpl} map type,
 * that allows querying members using address or uuid.
 */
final class MemberMap {

    private final int version;
    private final Map<Address, MemberImpl> addressToMemberMap;
    private final Map<String, MemberImpl> uuidToMemberMap;
    private final Set<MemberImpl> members;

    MemberMap(int version, Map<Address, MemberImpl> addressMap, Map<String, MemberImpl> uuidMap) {
        this.version = version;
        assert new HashSet<MemberImpl>(addressMap.values()).equals(new HashSet<MemberImpl>(uuidMap.values()))
                : "Maps are different! AddressMap: " + addressMap + ", UuidMap: " + uuidMap;

        this.addressToMemberMap = addressMap;
        this.uuidToMemberMap = uuidMap;
        this.members = Collections.unmodifiableSet(new LinkedHashSet<MemberImpl>(addressToMemberMap.values()));
    }

    /**
     * Creates an empty {@code MemberMap}.
     *
     * @return empty {@code MemberMap}
     */
    static MemberMap empty() {
        return new MemberMap(0, Collections.<Address, MemberImpl>emptyMap(), Collections.<String, MemberImpl>emptyMap());
    }

    /**
     * Creates a singleton {@code MemberMap} including only specified member.
     *
     * @param member sole member in map
     * @return singleton {@code MemberMap}
     */
    static MemberMap singleton(MemberImpl member) {
        return new MemberMap(1, singletonMap(member.getAddress(), member), singletonMap(member.getUuid(), member));
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param members members
     * @return a new {@code MemberMap}
     */
    static MemberMap createNew(MemberImpl... members) {
        return createNew(0, members);
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param version version
     * @param members members
     * @return a new {@code MemberMap}
     */
    static MemberMap createNew(int version, MemberImpl... members) {
        Map<Address, MemberImpl> addressMap = new LinkedHashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new LinkedHashMap<String, MemberImpl>();

        for (MemberImpl member : members) {
            putMember(addressMap, uuidMap, member);
        }

        return new MemberMap(version, addressMap, uuidMap);
    }

    /**
     * Creates clone of source {@code MemberMap}, excluding given members.
     * If source is empty, same map instance will be returned. If excluded members are empty or not present in
     * source, a new map will be created containing the same members with source.
     *
     * @param source         source map
     * @param excludeMembers members to exclude
     * @return clone map
     */
    static MemberMap cloneExcluding(MemberMap source, MemberImpl... excludeMembers) {
        if (source.size() == 0) {
            return source;
        }

        Map<Address, MemberImpl> addressMap = new LinkedHashMap<Address, MemberImpl>(source.addressToMemberMap);
        Map<String, MemberImpl> uuidMap = new LinkedHashMap<String, MemberImpl>(source.uuidToMemberMap);

        for (MemberImpl member : excludeMembers) {
            MemberImpl removed = addressMap.remove(member.getAddress());
            if (removed != null) {
                uuidMap.remove(removed.getUuid());
            }

            removed = uuidMap.remove(member.getUuid());
            if (removed != null) {
                addressMap.remove(removed.getAddress());
            }
        }

        return new MemberMap(source.version + 1, addressMap, uuidMap);
    }

    /**
     * Creates clone of source {@code MemberMap} additionally including new members.
     *
     * @param source     source map
     * @param newMembers new members to add
     * @return clone map
     */
    static MemberMap cloneAdding(MemberMap source, MemberImpl... newMembers) {
        Map<Address, MemberImpl> addressMap = new LinkedHashMap<Address, MemberImpl>(source.addressToMemberMap);
        Map<String, MemberImpl> uuidMap = new LinkedHashMap<String, MemberImpl>(source.uuidToMemberMap);

        for (MemberImpl member : newMembers) {
            putMember(addressMap, uuidMap, member);
        }

        return new MemberMap(source.version + 1, addressMap, uuidMap);
    }

    private static void putMember(Map<Address, MemberImpl> addressMap,
                                  Map<String, MemberImpl> uuidMap, MemberImpl member) {

        MemberImpl current = addressMap.put(member.getAddress(), member);
        if (current != null) {
            throw new IllegalArgumentException("Replacing existing member with address: " + member);
        }

        current = uuidMap.put(member.getUuid(), member);
        if (current != null) {
            throw new IllegalArgumentException("Replacing existing member with uuid: " + member);
        }
    }

    MemberImpl getMember(Address address) {
        return addressToMemberMap.get(address);
    }

    MemberImpl getMember(String uuid) {
        return uuidToMemberMap.get(uuid);
    }

    boolean contains(Address address) {
        return addressToMemberMap.containsKey(address);
    }

    boolean contains(String uuid) {
        return uuidToMemberMap.containsKey(uuid);
    }

    Set<MemberImpl> getMembers() {
        return members;
    }

    Collection<Address> getAddresses() {
        return unmodifiableCollection(addressToMemberMap.keySet());
    }

    int size() {
        return members.size();
    }

    int getVersion() {
        return version;
    }

    MembersView toMembersView() {
        return MembersView.createNew(version, members);
    }

    Set<MemberImpl> getMembersAfterFirstMember(MemberImpl first) {
        final Set<MemberImpl> filtered = new LinkedHashSet<MemberImpl>();
        Iterator<MemberImpl> it = this.members.iterator();
        MemberImpl member = null;
        while (it.hasNext()) {
            MemberImpl m = it.next();
            if (m.equals(first)) {
                member = m;
                break;
            }
        }

        checkNotNull(member, "Member: " + first + " not found in member list!");

        filtered.add(member);
        while (it.hasNext()) {
            filtered.add(it.next());
        }

        return filtered;
    }

    Set<MemberImpl> getMembersBeforeMember(Address member) {
        if (!addressToMemberMap.containsKey(member)) {
            throw new IllegalArgumentException(member + " is not in the member list!");
        }

        final Set<MemberImpl> before = new LinkedHashSet<MemberImpl>();
        for (MemberImpl m : this.members) {
            if (m.getAddress().equals(member)) {
                break;
            } else {
                before.add(m);
            }
        }

        return before;
    }

}
