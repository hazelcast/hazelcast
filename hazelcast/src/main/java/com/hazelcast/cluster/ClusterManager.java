/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.impl.*;
import com.hazelcast.impl.base.Call;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.*;
import com.hazelcast.util.Prioritized;

import java.util.*;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public final class ClusterManager extends BaseManager implements ConnectionListener {

    private final long WAIT_MILLIS_BEFORE_JOIN;

    private final long MAX_NO_HEARTBEAT_MILLIS;

    private final Set<ScheduledAction> setScheduledActions = new LinkedHashSet<ScheduledAction>(1000);

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private final List<MemberImpl> lsMembersBefore = new ArrayList<MemberImpl>();

    public ClusterManager(final Node node) {
        super(node);
        WAIT_MILLIS_BEFORE_JOIN = node.groupProperties.WAIT_SECONDS_BEFORE_JOIN.getInteger() * 1000L;
        MAX_NO_HEARTBEAT_MILLIS = node.groupProperties.MAX_NO_HEARTBEAT_SECONDS.getInteger() * 1000L;
        node.clusterService.registerPeriodicRunnable(new Runnable() {
            public void run() {
                heartBeater();
            }
        });
        node.clusterService.registerPeriodicRunnable(new Runnable() {
            public void run() {
                checkScheduledActions();
            }
        });
        node.connectionManager.addConnectionListener(this);
        registerPacketProcessor(ClusterOperation.RESPONSE, new PacketProcessor() {
            public void process(Packet packet) {
                handleResponse(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.HEARTBEAT, new PacketProcessor() {
            public void process(Packet packet) {
                releasePacket(packet);
            }
        });
        registerPacketProcessor(ClusterOperation.REMOTELY_PROCESS_AND_RESPOND,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Data data = packet.getValueData();
                        RemotelyProcessable rp = (RemotelyProcessable) toObject(data);
                        rp.setConnection(packet.conn);
                        rp.setNode(node);
                        rp.process();
                        sendResponse(packet);
                    }
                });
        registerPacketProcessor(ClusterOperation.REMOTELY_PROCESS,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Data data = packet.getValueData();
                        RemotelyProcessable rp = (RemotelyProcessable) toObject(data);
                        rp.setConnection(packet.conn);
                        rp.setNode(node);
                        rp.process();
                        releasePacket(packet);
                    }
                });
        registerPacketProcessor(ClusterOperation.REMOTELY_CALLABLE_BOOLEAN,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Boolean result;
                        AbstractRemotelyCallable<Boolean> callable = null;
                        try {
                            Data data = packet.getValueData();
                            callable = (AbstractRemotelyCallable<Boolean>) toObject(data);
                            callable.setConnection(packet.conn);
                            callable.setNode(node);
                            result = callable.call();
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Error processing " + callable, e);
                            result = Boolean.FALSE;
                        }
                        if (result == Boolean.TRUE) {
                            sendResponse(packet);
                        } else {
                            sendResponseFailure(packet);
                        }
                    }
                });
        registerPacketProcessor(ClusterOperation.REMOTELY_CALLABLE_OBJECT,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Object result;
                        AbstractRemotelyCallable<Boolean> callable = null;
                        try {
                            Data data = packet.getValueData();
                            callable = (AbstractRemotelyCallable) toObject(data);
                            callable.setConnection(packet.conn);
                            callable.setNode(node);
                            result = callable.call();
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Error processing " + callable, e);
                            result = null;
                        }
                        if (result != null) {
                            Data value;
                            if (result instanceof Data) {
                                value = (Data) result;
                            } else {
                                value = toData(result);
                            }
                            packet.setValue(value);
                        }
                        sendResponse(packet);
                    }
                });
    }

    public final void heartBeater() {
        if (!node.joined())
            return;
        long now = System.currentTimeMillis();
        if (isMaster()) {
            List<Address> lsDeadAddresses = null;
            for (MemberImpl memberImpl : lsMembers) {
                final Address address = memberImpl.getAddress();
                if (!thisAddress.equals(address)) {
                    try {
                        Connection conn = node.connectionManager.getConnection(address);
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastRead()) >= (MAX_NO_HEARTBEAT_MILLIS)) {
                                conn = null;
                                if (lsDeadAddresses == null) {
                                    lsDeadAddresses = new ArrayList<Address>();
                                }
                                logger.log(Level.WARNING, "Added " + address + " to list of dead addresses because of timeout since last read");
                                lsDeadAddresses.add(address);
                            }
                        }
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastWrite()) > 500) {
                                Packet packet = obtainPacket("heartbeat", null, null,
                                        ClusterOperation.HEARTBEAT, 0);
                                sendOrReleasePacket(packet, conn);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            }
            if (lsDeadAddresses != null) {
                for (Address address : lsDeadAddresses) {
                    logger.log(Level.FINEST, "NO HEARTBEAT should remove " + address);
                    doRemoveAddress(address);
                    sendRemoveMemberToOthers(address);
                }
            }
        } else {
            // send heartbeat to isMaster
            if (getMasterAddress() != null) {
                MemberImpl masterMember = getMember(getMasterAddress());
                boolean removed = false;
                if (masterMember != null) {
                    if ((now - masterMember.getLastRead()) >= (MAX_NO_HEARTBEAT_MILLIS)) {
                        logger.log(Level.FINEST, "Master node has timed out it's heartbeat and will be removed");
                        doRemoveAddress(getMasterAddress());
                        removed = true;
                    }
                }
                if (!removed) {
                    Packet packet = obtainPacket("heartbeat", null, null, ClusterOperation.HEARTBEAT,
                            0);
                    Connection connMaster = node.connectionManager.getOrConnect(getMasterAddress());
                    sendOrReleasePacket(packet, connMaster);
                }
            }
            for (MemberImpl member : lsMembers) {
                if (!member.localMember()) {
                    Address address = member.getAddress();
                    if (shouldConnectTo(address)) {
                        Connection conn = node.connectionManager.getOrConnect(address);
                        if (conn != null) {
                            Packet packet = obtainPacket("heartbeat", null, null,
                                    ClusterOperation.HEARTBEAT, 0);
                            sendOrReleasePacket(packet, conn);
                        } else {
                            logger.log(Level.FINEST, "could not connect to " + address + " to send heartbeat");
                        }
                    } else {
                        Connection conn = node.connectionManager.getConnection(address);
                        if (conn != null && conn.live()) {
                            if ((now - member.getLastWrite()) > 500) {
                                Packet packet = obtainPacket("heartbeat", null, null,
                                        ClusterOperation.HEARTBEAT, 0);
                                sendOrReleasePacket(packet, conn);
                            }
                        } else {
                            logger.log(Level.FINEST, "not sending heartbeat because connection is null or not live " + address);
                        }
                    }
                }
            }
        }
    }

    public boolean shouldConnectTo(Address address) {
        return !node.joined() || (lsMembers.indexOf(getMember(thisAddress)) > lsMembers.indexOf(getMember(address)));
    }

    private void sendRemoveMemberToOthers(final Address deadAddress) {
        for (MemberImpl member : lsMembers) {
            Address address = member.getAddress();
            if (!thisAddress.equals(address)) {
                if (!address.equals(deadAddress)) {
                    sendProcessableTo(new MemberRemover(deadAddress), address);
                }
            }
        }
    }

    public void handleMaster(Master master) {
        if (!node.joined() && !thisAddress.equals(master.address)) {
            node.setMasterAddress(master.address);
            Connection connMaster = node.connectionManager.getOrConnect(master.address);
            if (connMaster != null) {
                sendJoinRequest(master.address);
            }
        }
    }

    public void handleAddRemoveConnection(AddOrRemoveConnection connection) {
        if (connection.add) { // Just connect to the new address if not connected already.
            if (!connection.address.equals(thisAddress)) {
                node.connectionManager.getOrConnect(connection.address);
            }
        } else { // Remove dead member
            if (connection.address != null) {
                doRemoveAddress(connection.address);
            }
        } // end of REMOVE CONNECTION
    }

    void doRemoveAddress(Address deadAddress) {
        logger.log(Level.FINEST, "Removing Address " + deadAddress);
        if (!node.joined()) {
            node.failedConnection(deadAddress);
            return;
        }
        if (deadAddress.equals(thisAddress))
            return;
        if (deadAddress.equals(getMasterAddress())) {
            if (node.joined()) {
                MemberImpl newMaster = getNextMemberAfter(deadAddress, false, 1);
                if (newMaster != null)
                    node.setMasterAddress(newMaster.getAddress());
                else
                    node.setMasterAddress(null);
            } else {
                node.setMasterAddress(null);
            }
            logger.log(Level.FINEST, "Now Master " + node.getMasterAddress());
        }
        if (isMaster()) {
            setJoins.remove(new MemberInfo(deadAddress));
        }
        Connection conn = node.connectionManager.getConnection(deadAddress);
        if (conn != null) {
            node.connectionManager.remove(conn);
        }
        MemberImpl deadMember = getMember(deadAddress);
        if (deadMember != null) {
            lsMembersBefore.clear();
            for (MemberImpl memberBefore : lsMembers) {
                lsMembersBefore.add(memberBefore);
            }
            removeMember(deadAddress);
            node.blockingQueueManager.syncForDead(deadAddress);
            node.concurrentMapManager.syncForDead(deadMember);
            node.listenerManager.syncForDead(deadAddress);
            node.topicManager.syncForDead(deadAddress);
            node.getClusterImpl().setMembers(lsMembers);
            // toArray will avoid CME as onDisconnect does remove the calls
            Object[] calls = mapCalls.values().toArray();
            for (Object call : calls) {
                ((Call) call).onDisconnect(deadAddress);
            }
            logger.log(Level.INFO, this.toString());
        }
    }

    public List<MemberImpl> getMembersBeforeSync() {
        return lsMembersBefore;
    }

    public boolean isNextOrPreviousChanged() {
        return isNextChanged() || isPreviousChanged();
    }

    public boolean isNextChanged() {
        Member nextMemberBefore = getNextMemberBeforeSync(thisAddress, true, 1);
        Member nextMemberNow = getNextMemberAfter(thisAddress, true, 1);
        if (nextMemberBefore == null) {
            return (nextMemberNow != null);
        } else {
            return (!nextMemberBefore.equals(nextMemberNow));
        }
    }

    public boolean isPreviousChanged() {
        int indexBefore = (lsMembersBefore.indexOf(thisMember));
        int indexNow = (lsMembers.indexOf(thisMember));
        MemberImpl previousMemberBefore = getMemberAt(lsMembersBefore, (indexBefore - 1));
        MemberImpl previousMemberNow = getMemberAt(lsMembers, (indexNow - 1));
        if (previousMemberBefore == null) {
            return (previousMemberNow != null);
        } else {
            return (!previousMemberBefore.equals(previousMemberNow));
        }
    }

    public MemberImpl getMemberAt(List<MemberImpl> members, int index) {
        return members.get((index + members.size()) % members.size());
    }

    void handleJoinRequest(JoinRequest joinRequest) {
        logger.log(Level.FINEST, joinInProgress + " Handling " + joinRequest);
        if (getMember(joinRequest.address) != null)
            return;
        Connection conn = joinRequest.getConnection();
        if (node.validateJoinRequest(joinRequest)) {
            if (!node.getConfig().getNetworkConfig().getJoin().getMulticastConfig().isEnabled()) {
                if (node.getMasterAddress() != null && !isMaster()) {
                    sendProcessableTo(new Master(node.getMasterAddress()), conn);
                }
            }
            if (isMaster()) {
                if (joinRequest.to != null && !joinRequest.to.equals(thisAddress)) {
                    sendProcessableTo(new Master(node.getMasterAddress()), conn);
                    return;
                }
                if (!joinInProgress) {
                    MemberInfo newMemberInfo = new MemberInfo(joinRequest.address, joinRequest.nodeType);
                    if (setJoins.add(newMemberInfo)) {
                        sendProcessableTo(new Master(node.getMasterAddress()), conn);
                        timeToStartJoin = System.currentTimeMillis() + WAIT_MILLIS_BEFORE_JOIN;
                    } else {
                        if (System.currentTimeMillis() > timeToStartJoin) {
                            startJoin();
                        }
                    }
                }
            }
        } else {
            conn.close();
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("\n\nMembers [");
        sb.append(lsMembers.size());
        sb.append("] {");
        for (MemberImpl member : lsMembers) {
            sb.append("\n\t").append(member);
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    public int getMemberDistance(Address target) {
        int indexThis = -1;
        int indexTarget = -1;
        int index = 0;
        for (MemberImpl member : lsMembers) {
            if (member.getAddress().equals(thisAddress)) {
                indexThis = index;
            } else if (member.getAddress().equals(target)) {
                indexTarget = index;
            }
            if (indexThis > -1 && indexTarget > -1) {
                int distance = indexThis - indexTarget;
                if (distance < 0) {
                    distance += lsMembers.size();
                }
                return distance;
            }
            index++;
        }
        return 0;
    }

    public Packet createRemotelyProcessablePacket(RemotelyProcessable rp) {
        Data value = ThreadContext.get().toData(rp);
        Packet packet = obtainPacket();
        packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
        return packet;
    }

    public void sendProcessableTo(RemotelyProcessable rp, Connection conn) {
        Packet packet = createRemotelyProcessablePacket(rp);
        boolean sent = send(packet, conn);
        if (!sent) {
            releasePacket(packet);
        }
    }

    void joinReset() {
        joinInProgress = false;
        setJoins.clear();
        timeToStartJoin = System.currentTimeMillis() + WAIT_MILLIS_BEFORE_JOIN;
    }

    public class AsyncRemotelyBooleanCallable extends TargetAwareOp {
        AbstractRemotelyCallable<Boolean> arp = null;

        public void executeProcess(Address address, AbstractRemotelyCallable<Boolean> arp) {
            this.arp = arp;
            super.target = address;
            arp.setNode(node);
            setLocal(ClusterOperation.REMOTELY_CALLABLE_BOOLEAN, "call", null, arp, 0, -1);
            request.setBooleanRequest();
            doOp();
        }

        @Override
        public Address getTarget() {
            return target;
        }

        @Override
        public void onDisconnect(final Address dead) {
            if (dead.equals(target)) {
                removeCall(getCallId());
                setResult(Boolean.FALSE);
            }
        }

        @Override
        public void process() {
            if (!thisAddress.equals(target) && node.connectionManager.getConnection(target) == null) {
                setResult(Boolean.FALSE);
            } else {
                super.process();
            }
        }

        @Override
        public void doLocalOp() {
            Boolean result;
            try {
                result = arp.call();
                setResult(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void setTarget() {
        }
    }

    public void finalizeJoin() {
        List<AsyncRemotelyBooleanCallable> calls = new ArrayList<AsyncRemotelyBooleanCallable>();
        for (final MemberImpl member : lsMembers) {
            if (!member.localMember()) {
                AsyncRemotelyBooleanCallable rrp = new AsyncRemotelyBooleanCallable();
                rrp.executeProcess(member.getAddress(), new FinalizeJoin());
                calls.add(rrp);
            }
        }
        for (AsyncRemotelyBooleanCallable call : calls) {
            call.getResultAsBoolean();
        }
    }

    class JoinRunnable implements Runnable, Prioritized {

        final MembersUpdateCall membersUpdate;

        JoinRunnable(MembersUpdateCall membersUpdate) {
            this.membersUpdate = membersUpdate;
        }

        public void run() {
            Collection<MemberInfo> lsMemberInfos = membersUpdate.getMemberInfos();
            List<Address> newMemberList = new ArrayList<Address>(lsMemberInfos.size());
            for (final MemberInfo memberInfo : lsMemberInfos) {
                newMemberList.add(memberInfo.address);
            }
            List<AsyncRemotelyBooleanCallable> calls = new ArrayList<AsyncRemotelyBooleanCallable>(lsMemberInfos.size());
            for (final Address target : newMemberList) {
                AsyncRemotelyBooleanCallable rrp = new AsyncRemotelyBooleanCallable();
                rrp.executeProcess(target, membersUpdate);
                calls.add(rrp);
            }
            for (AsyncRemotelyBooleanCallable call : calls) {
                if (call.getResultAsBoolean() == Boolean.FALSE) {
                    newMemberList.remove(call.getTarget());
                }
            }
            calls.clear();
            for (final Address target : newMemberList) {
                AsyncRemotelyBooleanCallable call = new AsyncRemotelyBooleanCallable();
                call.executeProcess(target, new SyncProcess());
                calls.add(call);
            }
            for (AsyncRemotelyBooleanCallable call : calls) {
                if (call.getResultAsBoolean() == Boolean.FALSE) {
                    newMemberList.remove(call.getTarget());
                }
            }
            calls.clear();
            AbstractRemotelyCallable<Boolean> connCheckCallable = new ConnectionCheckCall();
            for (final Address target : newMemberList) {
                AsyncRemotelyBooleanCallable call = new AsyncRemotelyBooleanCallable();
                call.executeProcess(target, connCheckCallable);
                calls.add(call);
            }
            for (AsyncRemotelyBooleanCallable call : calls) {
                if (call.getResultAsBoolean() == Boolean.FALSE) {
                    newMemberList.remove(call.getTarget());
                }
            }
        }
    }

    void startJoin() {
        joinInProgress = true;
        final MembersUpdateCall membersUpdate = new MembersUpdateCall(lsMembers, node.getClusterImpl().getClusterTime());
        if (setJoins != null && setJoins.size() > 0) {
            for (MemberInfo memberJoined : setJoins) {
                membersUpdate.addMemberInfo(memberJoined);
            }
        }
        node.executorManager.executeMigrationTask(new JoinRunnable(membersUpdate));
    }

    void updateMembers(Collection<MemberInfo> lsMemberInfos) {
        logger.log(Level.FINEST, "MEMBERS UPDATE!!");
        // Copy lsMembers to lsMembersBefore
        lsMembersBefore.clear();
        Map<Address, MemberImpl> mapOldMembers = new HashMap<Address, MemberImpl>();
        for (MemberImpl member : lsMembers) {
            lsMembersBefore.add(member);
            mapOldMembers.put(member.getAddress(), member);
        }
        lsMembers.clear();
        mapMembers.clear();
        for (MemberInfo memberInfo : lsMemberInfos) {
            MemberImpl member = mapOldMembers.get(memberInfo.address);
            if (member == null) {
                member = addMember(memberInfo.address, memberInfo.nodeType);
            } else {
                addMember(member);
            }
            member.didRead();
        }
        if (!lsMembers.contains(thisMember)) {
            throw new RuntimeException("Member list doesn't contain local member!");
        }
        heartBeater();
        node.getClusterImpl().setMembers(lsMembers);
        node.unlock();
        logger.log(Level.INFO, this.toString());
    }

    public void sendJoinRequest(Address toAddress) {
        if (toAddress == null) {
            toAddress = node.getMasterAddress();
        }
        sendProcessableTo(new JoinRequest(thisAddress, node.getConfig().getGroupConfig().getName(),
                node.getConfig().getGroupConfig().getPassword(),
                node.getLocalNodeType(), Packet.PACKET_VERSION, node.getBuildNumber()),
                toAddress);
    }

    public void registerScheduledAction(ScheduledAction scheduledAction) {
        setScheduledActions.add(scheduledAction);
    }

    public void deregisterScheduledAction(ScheduledAction scheduledAction) {
        setScheduledActions.remove(scheduledAction);
    }

    public void checkScheduledActions() {
        if (setScheduledActions.size() > 0) {
            Iterator<ScheduledAction> it = setScheduledActions.iterator();
            while (it.hasNext()) {
                ScheduledAction sa = it.next();
                if (sa.expired()) {
                    sa.onExpire();
                    it.remove();
                }
            }
        }
    }

    public RemotelyProcessable createInitialProcess() {
        InitialProcess initialProcess = new InitialProcess();
        List<AbstractRemotelyProcessable> lsProcessables = initialProcess.getProcessables();
        node.listenerManager.collectInitialProcess(lsProcessables);
        return initialProcess;
    }

    public void connectionAdded(final Connection connection) {
        enqueueAndReturn(new Processable() {
            public void process() {
                MemberImpl member = getMember(connection.getEndPoint());
                if (member != null) {
                    member.didRead();
                }
                if (!thisAddress.equals(connection.getEndPoint())) {
                    sendProcessableTo(createInitialProcess(), connection);
                }
            }
        });
    }

    public void connectionRemoved(Connection connection) {
        logger.log(Level.FINEST, "Connection is removed " + connection.getEndPoint());
        if (!node.joined()) {
            if (getMasterAddress() != null) {
                if (getMasterAddress().equals(connection.getEndPoint())) {
                    node.setMasterAddress(null);
                }
            }
        }
    }

    public Member addMember(MemberImpl member) {
        logger.log(Level.FINEST, "ClusterManager adding " + member);
        if (lsMembers.contains(member)) {
            for (MemberImpl m : lsMembers) {
                if (m.equals(member)) {
                    member = m;
                }
            }
        } else {
            if (!member.getAddress().equals(thisAddress)) {
                node.connectionManager.getConnection(member.getAddress());
            }
            lsMembers.add(member);
            mapMembers.put(member.getAddress(), member);
        }
        return member;
    }

    protected void removeMember(Address address) {
        logger.log(Level.FINEST, "removing  " + address);
        MemberImpl member = mapMembers.remove(address);
        if (member != null) {
            lsMembers.remove(member);
        }
    }

    protected MemberImpl createMember(Address address, NodeType nodeType) {
        return new MemberImpl(address, thisAddress.equals(address), nodeType);
    }

    public MemberImpl getMember(Address address) {
        return mapMembers.get(address);
    }

    final public MemberImpl addMember(Address address, NodeType nodeType) {
        if (address == null) {
            logger.log(Level.FINEST, "Address cannot be null");
            return null;
        }
        MemberImpl member = getMember(address);
        if (member == null) {
            member = createMember(address, nodeType);
        }
        addMember(member);
        return member;
    }

    public void stop() {
        if (setJoins != null) {
            setJoins.clear();
        }
        timeToStartJoin = 0;
        if (lsMembers != null) {
            lsMembers.clear();
        }
        if (mapMembers != null) {
            mapMembers.clear();
        }
        if (lsMembersBefore != null) {
            lsMembersBefore.clear();
        }
        if (mapCalls != null) {
            mapCalls.clear();
        }
    }
}
