/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.listener;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ClientSmartListenerService extends ClientListenerServiceImpl implements InitialMembershipListener {
    private static final long SMART_LISTENER_MEMBER_ADDED_RESCHEDULE_TIME = 1000;
    private static final long SMART_LISTENER_CONNECT_ALL_SERVERS_RETRY_WAIT_TIME = 5000;
    private final Set<Member> members = new HashSet<Member>();
    // The value for the entry is a map of registrations where the key is the uuid string of the member
    private final Map<ClientRegistrationKey, Map<Member, ClientEventRegistration>> registrations
            = new ConcurrentHashMap<ClientRegistrationKey, Map<Member, ClientEventRegistration>>();
    private final ClientClusterService clusterService;
    private final int invocationTimeout;
    private volatile LifecycleEvent.LifecycleState lifecycleState;
    private String membershipListenerId;
    private ScheduledFuture<?> connectionOpener;

    public ClientSmartListenerService(HazelcastClientInstanceImpl client,
                                      int eventThreadCount, int eventQueueCapacity) {
        super(client, eventThreadCount, eventQueueCapacity);
        clusterService = client.getClientClusterService();
        invocationTimeout = client.getClientConfig().getNetworkConfig().getConnectionTimeout();
    }

    @Override
    public String registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        Future<String> future = registrationExecutor.submit(new Callable<String>() {
            @Override
            public String call() {
                String userRegistrationId = UuidUtil.newUnsecureUuidString();

                ClientRegistrationKey registrationKey = new ClientRegistrationKey(userRegistrationId, handler, codec);

                return register(registrationKey);
            }
        });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private String register(ClientRegistrationKey registrationKey) {
        registrations.put(registrationKey, new ConcurrentHashMap<Member, ClientEventRegistration>());
        for (Member member : members) {
            try {
                invoke(registrationKey, member);
            } catch (Exception e) {
                try {
                    deregister(registrationKey, getMemberUuids());
                } catch (Exception cleanupException) {
                    logger.warning("Could not perform appropriate cleanup for " + registrationKey, cleanupException);
                }
                throw new HazelcastException("Listener " + registrationKey + " can not be added to member " + member, e);
            }
        }
        return registrationKey.getUserRegistrationId();
    }

    @Override
    public void onClusterConnect(final ClientConnection clientConnection) {
        try {
            registrationExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Collection<Member> newMemberList = client.getClientClusterService().getMemberList();
                    members.clear();
                    members.addAll(newMemberList);

                    if (registrations.isEmpty()) {
                        return;
                    }

                    reRegisterAll();
                }
            }).get();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
    }

    private void invoke(ClientRegistrationKey registrationKey, Member member) throws Exception {
        ListenerMessageCodec codec = registrationKey.getCodec();
        ClientMessage request = codec.encodeAddRequest(true);
        EventHandler handler = registrationKey.getHandler();
        handler.beforeListenerRegister();

        Address address = member.getAddress();
        ClientInvocation invocation = new ClientInvocation(client, request, address);
        invocation.setEventHandler(handler);
        String serverRegistrationId = codec.decodeAddResponse(invocation.invoke().get(invocationTimeout, TimeUnit.MILLISECONDS));

        handler.onListenerRegister();
        long correlationId = request.getCorrelationId();
        ClientEventRegistration registration
                = new ClientEventRegistration(serverRegistrationId, correlationId, member, codec);

        Map<Member, ClientEventRegistration> registrationMap = registrations.get(registrationKey);
        registrationMap.put(member, registration);
    }

    @Override
    public boolean deregisterListener(final String userRegistrationId) {
        try {
            Future<Boolean> future = registrationExecutor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    ClientRegistrationKey key = new ClientRegistrationKey(userRegistrationId);
                    return deregister(key, getMemberUuids());
                }
            });

            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    /**
     * Should be called while in the registration executor
     * @param key The key of the registration
     * @param memberUuids The uuids of the members in the cluster
     * @return true if deregistered the registration from the cluster
     */
    private Boolean deregister(ClientRegistrationKey key, Set<String> memberUuids) {
        Map<Member, ClientEventRegistration> registrationMap = registrations.get(key);
        if (registrationMap == null) {
            return false;
        }
        boolean successful = true;
        for (ClientEventRegistration registration : registrationMap.values()) {
            Member subscriber = registration.getSubscriber();
            try {
                // We need to compare uuids since the member may reconnect to the restarted member which restarted at the same
                // address and member.equals return true but they are actually different instances.
                if (memberUuids.contains(subscriber.getUuid())) {
                    ClientInvocationFuture invocationFuture = getClientInvocationFuture(registration, subscriber);
                    invocationFuture.get(invocationTimeout, TimeUnit.MILLISECONDS);
                } else {
                    // just send the invocation and do not wait for response and suppress any exceptions
                    try {
                        getClientInvocationFuture(registration, subscriber);
                    } catch (Exception e) {
                        logger.finest("Suppressing the exception during listener deregistration invocation for registration:"
                                + registration + ", since registered member " + subscriber + " is not in the members list "
                                + members, e);
                    }
                }
                removeEventHandler(registration.getCallId());
                registrationMap.remove(subscriber);
            } catch (Exception e) {
                successful = false;
                logger.warning("Deregistration of listener with id " + key.getUserRegistrationId()
                        + " has failed to member " + subscriber, e);
            }
        }
        if (successful) {
            registrations.remove(key);
        }
        return successful;
    }

    /**
     * Should be called while in the registration executor
     * @param registration The registration for which the deregister invocation will be sent
     * @param subscriber The member added from which to deregister
     * @return The future for the invocation.
     */
    private ClientInvocationFuture getClientInvocationFuture(ClientEventRegistration registration, Member subscriber) {
        ClientInvocationFuture invocationFuture;
        ListenerMessageCodec listenerMessageCodec = registration.getCodec();
        String serverRegistrationId = registration.getServerRegistrationId();
        ClientMessage request = listenerMessageCodec.encodeRemoveRequest(serverRegistrationId);
        invocationFuture = new ClientInvocation(client, request, subscriber.getAddress()).invoke();

        logger.finest("Invoked deregister listener invocation for " + registration + " to member " + subscriber);
        return invocationFuture;
    }

    private Set<String> getMemberUuids() {
        Set<String> memberUuids = new HashSet<String>(members.size());
        for (Member m : members) {
            memberUuids.add(m.getUuid());
        }
        return memberUuids;
    }

    @Override
    public void start() {
        membershipListenerId = clusterService.addMembershipListener(this);
        if (null != clusterService.getOwnerConnectionAddress()) {
            lifecycleState = LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
        }
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                lifecycleState = event.getState();
            }
        });

        connectionOpener = client.getClientExecutionService().scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                registrationExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        ensureConnectionsToAllServers();
                    }
                });
            }
        }, 0, SMART_LISTENER_CONNECT_ALL_SERVERS_RETRY_WAIT_TIME, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        if (null != connectionOpener) {
            connectionOpener.cancel(true);
        }
        super.shutdown();
        if (membershipListenerId != null) {
            clusterService.removeMembershipListener(membershipListenerId);
        }
    }

    private final class MemberAddedHandler implements Runnable {
        final MembershipEvent membershipEvent;

        public MemberAddedHandler(MembershipEvent membershipEvent) {
            this.membershipEvent = membershipEvent;
        }

        /**
         * Warning: Make sure that this method is only be executed by the registrationExecutor.
         */
        @Override
        public void run() {
            if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED != lifecycleState) {
                logger.finest("Ignoring member added event " + membershipEvent + " since the client is disconnected.");
                return;
            }

            Member member = membershipEvent.getMember();
            if (members.contains(member)) {
                logger.finest("Ignoring member added event " + membershipEvent + " since the member is already in the list.");
                return;
            }

            logger.finest("New member added to the cluster. Registering " + registrations.size() + " listeners to member "
                    + member);

            try {
                getOrConnect(member, client.getClientClusterService().getOwnerConnectionAddress());
            } catch (Exception e) {
                logger.warning("Failed to register listeners to member " + member + " rescheduling the registration in "
                        + SMART_LISTENER_MEMBER_ADDED_RESCHEDULE_TIME + " msecs", e);

                client.getClientExecutionService().schedule(new Runnable() {
                    @Override
                    public void run() {
                        memberAdded(membershipEvent);
                    }
                }, SMART_LISTENER_MEMBER_ADDED_RESCHEDULE_TIME, TimeUnit.MILLISECONDS);
                return;
            }

            members.add(member);

            for (Map.Entry<ClientRegistrationKey, Map<Member, ClientEventRegistration>> entry : registrations.entrySet()) {
                ClientRegistrationKey registrationKey = entry.getKey();
                Map<Member, ClientEventRegistration> registrationMap = entry.getValue();
                // Only register if not already registered
                if (null == registrationMap.get(member)) {
                    try {
                        invoke(registrationKey, member);
                    } catch (Exception e) {
                        logger.warning("Listener " + registrationKey + " can not be added to new member " + member
                                        + " rescheduling the registration in " + SMART_LISTENER_MEMBER_ADDED_RESCHEDULE_TIME
                                + " msecs", e);
                        client.getClientExecutionService().schedule(new Runnable() {
                            @Override
                            public void run() {
                                memberAdded(membershipEvent);
                            }
                        }, SMART_LISTENER_MEMBER_ADDED_RESCHEDULE_TIME, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }
    }

    @Override
    public void memberAdded(final MembershipEvent membershipEvent) {
        registrationExecutor.submit(new MemberAddedHandler(membershipEvent));
    }

    @Override
    public void memberRemoved(final MembershipEvent membershipEvent) {
        registrationExecutor.submit(new Runnable() {
            @Override
            public void run() {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED != lifecycleState) {
                    logger.finest("Ignoring member removed event " + membershipEvent + " since the client is disconnected.");
                    return;
                }

                Member member = membershipEvent.getMember();
                members.remove(member);
                for (Map<Member, ClientEventRegistration> registrationMap : registrations.values()) {
                    removeRegistrationLocally(member, registrationMap);
                }
            }
        });
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        //nothing to do
    }

    @Override
    public void init(final InitialMembershipEvent event) {
    }

    private void reRegisterAll() {
        for (Map.Entry<ClientRegistrationKey, Map<Member, ClientEventRegistration>> entry : registrations.entrySet()) {
            ClientRegistrationKey key = entry.getKey();
            logger.finest("Reregistering listener " + key + " to the cluster.");

            deregister(key, getMemberUuids());

            try {
                register(key);
            } catch (Exception e) {
                Map<Member, ClientEventRegistration> registrationMap = entry.getValue();
                if (null == registrationMap) {
                    // put back an empty map for keeping the key to be registered in the next try
                    registrations.put(key, new ConcurrentHashMap<Member, ClientEventRegistration>());
                }
                ExceptionUtil.rethrow(e);
            }

            logger.finest("Reregistered listener " + key + " to the cluster.");
        }
    }

    /**
     * Warning: Should be called from the registrationExecutor.
     * @param member The member for which the registration is to be removed.
     * @param registrationMap The registrations from which to remove the registrations.
     */
    private void removeRegistrationLocally(Member member, Map<Member, ClientEventRegistration> registrationMap) {
        ClientEventRegistration registration = registrationMap.remove(member);
        if (null != registration) {
            removeEventHandler(registration.getCallId());
        }
    }

    private void ensureConnectionsToAllServers() {
        if (registrations.isEmpty()) {
            return;
        }

        Address ownerConnectionAddress = clusterService.getOwnerConnectionAddress();
        if (null == ownerConnectionAddress) {
            return;
        }

        for (Member member : members) {
            try {
                getOrConnect(member, ownerConnectionAddress);
            } catch (Exception e) {
                logger.warning("Could not open connection to member " + member, e);
            }
        }
    }

    private void getOrConnect(Member member, Address ownerConnectionAddress)
            throws IOException {
        Address memberAddress = member.getAddress();
        client.getConnectionManager().getOrConnect(memberAddress, ownerConnectionAddress.equals(memberAddress));
    }

    //For Testing
    public Collection<ClientEventRegistration> getActiveRegistrations(final String uuid) {
        Future<Collection<ClientEventRegistration>> future = registrationExecutor.submit(
                new Callable<Collection<ClientEventRegistration>>() {
                    @Override
                    public Collection<ClientEventRegistration> call() {
                        ClientRegistrationKey key = new ClientRegistrationKey(uuid);
                        Map<Member, ClientEventRegistration> registrationMap = registrations.get(key);
                        if (registrationMap == null) {
                            return Collections.EMPTY_LIST;
                        }
                        LinkedList<ClientEventRegistration> activeRegistrations = new LinkedList<ClientEventRegistration>();
                        for (ClientEventRegistration registration : registrationMap.values()) {
                            for (Member member : members) {
                                if (member.equals(registration.getSubscriber())) {
                                    activeRegistrations.add(registration);
                                }
                            }
                        }
                        return activeRegistrations;
                    }
                });
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}
