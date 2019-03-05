/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSessionManagementService;

/**
 * The CP subsystem is a component of a Hazelcast cluster that builds
 * an in-memory strongly consistent layer. It is accessed via
 * {@link HazelcastInstance#getCPSubsystem()}. Its data structures are CP with
 * respect to the CAP principle, i.e., they always maintain linearizability
 * and prefer consistency over availability during network partitions.
 * <p>
 * Currently, the CP subsystem contains only the implementations of Hazelcast's
 * concurrency APIs. These APIs do not maintain large states. For this reason,
 * all members of a Hazelcast cluster do not take part in the CP subsystem.
 * The number of members that takes part in the CP subsystem is specified with
 * {@link CPSubsystemConfig#setCPMemberCount(int)}. Let's suppose the number of
 * CP members is configured as C. Then, when Hazelcast cluster starts,
 * the first C members form the CP subsystem. These members are called
 * CP members and they can also contain data for other regular Hazelcast data
 * structures, such as {@link IMap}, {@link ISet}, etc.
 * <p>
 * Data structures in the CP subsystem run in {@link CPGroup}s. A CP group
 * consists of an odd number of {@link CPMember}s between 3 and 7.
 * Each CP group independently runs the Raft consensus algorithm. Operations
 * are committed & executed only after they are successfully replicated to
 * the majority of the CP members in a CP group. For instance, in a CP group of
 * 5 CP members, operations are committed when they are replicated to at least
 * 3 CP members. The size of CP groups are specified via
 * {@link CPSubsystemConfig#setGroupSize(int)} and each CP group contains
 * the same number of CP members.
 * <p>
 * Please note that the size of CP groups do not have to be same with
 * the CP member count. Namely, number of CP members in the CP subsystem can be
 * larger than the configured CP group size. In this case, CP groups will be
 * formed by selecting the CP members randomly. Please note that the current CP
 * subsystem implementation works only in memory without persisting any state
 * to disk. It means that a crashed CP member will not be able to recover by
 * reloading its previous state. Therefore, crashed CP members create a danger
 * for gradually losing majority of CP groups and eventually cause the total
 * loss of availability of the CP subsystem. To prevent such situations, failed
 * CP members can be removed from the CP subsystem and replaced in CP groups
 * with other available CP members. This flexibility provides a good degree of
 * fault-tolerance at run-time. Please see {@link CPSubsystemConfig} and
 * {@link CPSubsystemManagementService} for more details.
 * <p>
 * The CP subsystem runs 2 CP groups by default. The first one is
 * the Metadata group. It is an internal CP group which is responsible for
 * managing the CP members and CP groups. It is be initialized during the
 * cluster startup process if the CP subsystem is enabled via
 * {@link CPSubsystemConfig#setCPMemberCount(int)} configuration.
 * The second group is the DEFAULT CP group, whose name is given in
 * {@link CPGroup#DEFAULT_GROUP_NAME}. If a group name is not specified while
 * creating a proxy for a CP data structure, that data structure will be mapped
 * to the DEFAULT CP group. For instance, when a CP {@link IAtomicLong} instance
 * is created by calling {@code .getAtomicLong("myAtomicLong")}, it will be
 * initialized on the DEFAULT CP group. Besides these 2 pre-defined CP groups,
 * custom CP groups can be created at run-time. If a CP {@link IAtomicLong} is
 * created by calling {@code .getAtomicLong("myAtomicLong@myGroup")}, first
 * a new CP group will be created with the name "myGroup" and then
 * "myAtomicLong" will be initialized on this custom CP group.
 * <p>
 * The current set of CP data structures have quite low memory overheads.
 * Moreover, related to the Raft consensus algorithm, each CP group makes
 * uses of internal heartbeat RPCs to maintain authority of the leader member
 * and help lagging CP members to make progress. Last but not least, the new
 * CP lock and semaphore implementations rely on a brand new session mechanism.
 * In a nutshell, a Hazelcast server or a client starts a new session on the
 * corresponding CP group when it makes its very first lock or semaphore
 * acquire request, and then periodically commits session-heartbeats to this CP
 * group to indicate its liveliness. It means that if CP locks and semaphores
 * are distributed into multiple CP groups, there will be a session
 * management overhead. Please see {@link CPSession} for more details.
 * For the aforementioned reasons, we recommend developers to use a minimal
 * number of CP groups. For most use cases, the DEFAULT CP group should be
 * sufficient to maintain all CP data structure instances. Custom CP groups
 * could be created when throughput of the CP subsystem is needed to be
 * improved.
 * <p>
 * <strong>The CP data structure proxies differ from the other Hazelcast data
 * structure proxies in one aspect, that is, if you call the
 * {@link DistributedObject#destroy()} API on a CP data structure proxy,
 * that data structure is terminated on the underlying CP group and cannot be
 * reinitialized on the same CP group until the CP group is force-destroyed.
 * For this reason, please make sure that you are completely done with a CP
 * data structure before destroying its proxy.</strong>
 *
 * @see CPSubsystemConfig
 * @see CPMember
 * @see CPSession
 */
public interface CPSubsystem {

    /**
     * Returns a proxy for an {@link IAtomicLong} instance created on the CP
     * subsystem. Hazelcast's {@link IAtomicLong} is a distributed version of
     * <tt>java.util.concurrent.atomic.AtomicLong</tt>. If no group name is
     * given within the "name" parameter, then the {@link IAtomicLong} instance
     * will be created on the DEFAULT CP group. If a group name is given, like
     * {@code .getAtomicLong("myLong@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link IAtomicLong} instance will be created on this group. Returned
     * {@link IAtomicLong} instance offers linearizability and behaves as a CP
     * register. When a network partition occurs, proxies that exist on the
     * minority side of its CP group lose availability.
     *
     * @param name name of the {@link IAtomicLong} proxy
     * @return {@link IAtomicLong} proxy for the given name
     */
    IAtomicLong getAtomicLong(String name);

    /**
     * Returns a proxy for an {@link IAtomicReference} instance created on
     * the CP subsystem. Hazelcast's {@link IAtomicReference} is a distributed
     * version of <tt>java.util.concurrent.atomic.AtomicLong</tt>. If no group
     * name is given within the "name" parameter, then
     * the {@link IAtomicReference} instance will be created on the DEFAULT CP
     * group. If a group name is given, like
     * {@code .getAtomicReference("myRef@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link IAtomicReference} instance will be created on this group.
     * Returned {@link IAtomicReference} instance offers linearizability and
     * behaves as a CP register. When a network partition occurs, proxies that
     * exist on the minority side of its CP group lose availability.
     *
     * @param name name of the {@link IAtomicReference} proxy
     * @return {@link IAtomicReference} proxy for the given name
     */
    <E> IAtomicReference<E> getAtomicReference(String name);

    /**
     * Returns a proxy for an {@link ICountDownLatch} instance created on
     * the CP subsystem. Hazelcast's {@link ICountDownLatch} is a distributed
     * version of <tt>java.util.concurrent.CountDownLatch</tt>. If no group
     * name is given within the "name" parameter, then
     * the {@link ICountDownLatch} instance will be created on the DEFAULT CP
     * group. If a group name is given, like
     * {@code .getCountDownLatch("myLatch@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link ICountDownLatch} instance will be created on this group. Returned
     * {@link ICountDownLatch} instance offers linearizability. When a network
     * partition occurs, proxies that exist on the minority side of its CP
     * group lose availability.
     *
     * @param name name of the {@link ICountDownLatch} proxy
     * @return {@link ICountDownLatch} proxy for the given name
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * Returns a proxy for an {@link FencedLock} instance created on the CP
     * subsystem. Hazelcast's {@link FencedLock} is a distributed version of
     * <tt>java.util.concurrent.locks.Lock</tt>. If no group name is given
     * within the "name" parameter, then the {@link FencedLock} instance will
     * be created on the DEFAULT CP group. If a group name is given, like
     * {@code .getLock("myLock@group1")}, the given group will be initialized
     * first, if not initialized already, and then the {@link FencedLock}
     * instance will be created on this group. Returned {@link FencedLock}
     * instance offers linearizability. When a network partition occurs,
     * proxies that exist on the minority side of its CP group lose
     * availability.
     *
     * @param name name of the {@link FencedLock} proxy
     * @return {@link FencedLock} proxy for the given name
     */
    FencedLock getLock(String name);

    /**
     * Returns a proxy for an {@link ISemaphore} instance created on the CP
     * subsystem. Hazelcast's {@link ISemaphore} is a distributed version of
     * <tt>java.util.concurrent.Semaphore</tt>. If no group name is given
     * within the "name" parameter, then the {@link ISemaphore} instance will
     * be created on the DEFAULT CP group. If a group name is given, like
     * {@code .getSemaphore("mySemaphore@group1")}, the given group will be
     * initialized first, if not initialized already, and then the
     * {@link ISemaphore} instance will be created on this group. Returned
     * {@link ISemaphore} instance offers linearizability. When a network
     * partition occurs, proxies that exist on the minority side of its CP
     * group lose availability.
     *
     * @see CPSemaphoreConfig
     *
     * @param name name of the {@link ISemaphore} proxy
     * @return {@link ISemaphore} proxy for the given name
     */
    ISemaphore getSemaphore(String name);

    /**
     * Returns the local CP member if this Hazelcast member is part of
     * the CP subsystem, returns null otherwise.
     * <p>
     * This field is initialized when the local Hazelcast member is one of
     * the first {@link CPSubsystemConfig#getCPMemberCount()} members
     * in the cluster and the CP subsystem discovery process is completed.
     *
     * @return local CP member if available, null otherwise
     */
    CPMember getLocalCPMember();

    /**
     * Returns the {@link CPSubsystemManagementService} of this Hazelcast
     * instance. {@link CPSubsystemManagementService} offers APIs for managing
     * CP members and CP groups.
     *
     * @return the {@link CPSubsystemManagementService} of this Hazelcast instance
     */
    CPSubsystemManagementService getCPSubsystemManagementService();

    /**
     * Returns the {@link CPSessionManagementService} of this Hazelcast
     * instance. {@link CPSessionManagementService} offers APIs for managing CP
     * sessions.
     *
     * @return the {@link CPSessionManagementService} of this Hazelcast instance
     */
    CPSessionManagementService getCPSessionManagementService();

}
