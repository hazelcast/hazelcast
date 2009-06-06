/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Member;
import static com.hazelcast.impl.Constants.ClusterOperations.*;

import com.hazelcast.impl.BaseManager;
import com.hazelcast.impl.BlockingQueueManager;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.ListenerManager;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.TopicManager;
import com.hazelcast.impl.BaseManager.BooleanOp;
import com.hazelcast.impl.BaseManager.Call;
import com.hazelcast.impl.BaseManager.PacketProcessor;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.impl.BaseManager.ScheduledAction;
import com.hazelcast.impl.BaseManager.TargetAwareOp;
import com.hazelcast.nio.*;

import java.util.*;
import java.util.logging.Level;

public class ClusterManager extends BaseManager implements ConnectionListener {

    private static final ClusterManager instance = new ClusterManager();

    Set<ScheduledAction> setScheduledActions = new HashSet<ScheduledAction>(1000);

    public static ClusterManager get() {
        return instance;
    }

    private final Set<MemberInfo> setJoins = new LinkedHashSet<MemberInfo>(100);

    private boolean joinInProgress = false;

    private long timeToStartJoin = 0;

    private final List<MemberImpl> lsMembersBefore = new ArrayList<MemberImpl>();

    private final long waitTimeBeforeJoin = 5000;

    private ClusterManager() {
        ClusterService.get().registerPeriodicRunnable(new Runnable() {
            public void run() {
                heartBeater();
            }
        });
        ClusterService.get().registerPeriodicRunnable(new Runnable() {
            public void run() {
                checkScheduledActions();
            }
        });
        ConnectionManager.get().addConnectionListener(this);
        ClusterService.get().registerPacketProcessor(OP_RESPONSE, new PacketProcessor() {
            public void process(Packet packet) {
                handleResponse(packet);
            }
        });
        ClusterService.get().registerPacketProcessor(OP_HEARTBEAT, new PacketProcessor() {
            public void process(Packet packet) {
                packet.returnToContainer();
            }
        });
        ClusterService.get().registerPacketProcessor(OP_REMOTELY_PROCESS_AND_RESPOND,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Data data = BufferUtil.doTake(packet.value);
                        RemotelyProcessable rp = (RemotelyProcessable) ThreadContext.get()
                                .toObject(data);
                        rp.setConnection(packet.conn);
                        rp.process();
                        sendResponse(packet);
                    }
                });
        ClusterService.get().registerPacketProcessor(OP_REMOTELY_PROCESS,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Data data = BufferUtil.doTake(packet.value);
                        RemotelyProcessable rp = (RemotelyProcessable) ThreadContext.get()
                                .toObject(data);
                        rp.setConnection(packet.conn);
                        rp.process();
                        packet.returnToContainer();
                    }
                });

        ClusterService.get().registerPacketProcessor(OP_REMOTELY_BOOLEAN_CALLABLE,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Boolean result = null;
                        try {
                            Data data = BufferUtil.doTake(packet.value);
                            AbstractRemotelyCallable<Boolean> callable = (AbstractRemotelyCallable<Boolean>) ThreadContext
                                    .get().toObject(data);
                            callable.setConnection(packet.conn);
                            result = callable.call();
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                            result = Boolean.FALSE;
                        }
                        if (result == Boolean.TRUE) {
                            sendResponse(packet);
                        } else {
                            sendResponseFailure(packet);
                        }
                    }
                });

        ClusterService.get().registerPacketProcessor(OP_REMOTELY_OBJECT_CALLABLE,
                new PacketProcessor() {
                    public void process(Packet packet) {
                        Object result = null;
                        try {
                            Data data = BufferUtil.doTake(packet.value);
                            AbstractRemotelyCallable callable = (AbstractRemotelyCallable) ThreadContext
                                    .get().toObject(data);
                            callable.setConnection(packet.conn);
                            result = callable.call();
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                            result = null;
                        }
                        if (result != null) {
                            Data value = null;
                            if (result instanceof Data) {
                                value = (Data) result;
                            } else {
                                value = ThreadContext.get().toData(result);
                            }
                            BufferUtil.doSet(value, packet.value);
                        }

                        sendResponse(packet);
                    }
                });
    }

    public final void heartBeater() {
        if (!Node.get().joined())
            return;
        long now = System.currentTimeMillis();
        if (isMaster()) {
            List<Address> lsDeadAddresses = null;
            for (MemberImpl memberImpl : lsMembers) {
                final Address address = memberImpl.getAddress();
                if (!thisAddress.equals(address)) {
                    try {
                        Connection conn = ConnectionManager.get().getConnection(address);
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastRead()) >= 10000) {
                                conn = null;
                                if (lsDeadAddresses == null) {
                                    lsDeadAddresses = new ArrayList<Address>();
                                }
                                lsDeadAddresses.add(address);
                            }
                        }
                        if (conn != null && conn.live()) {
                            if ((now - memberImpl.getLastWrite()) > 500) {
                                Packet packet = obtainPacket("heartbeat", null, null,
                                        OP_HEARTBEAT, 0);
                                sendOrReleasePacket(packet, conn);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
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
            // send heartbeat to master
            if (getMasterAddress() != null) {
                MemberImpl masterMember = getMember(getMasterAddress());
                boolean removed = false;
                if (masterMember != null) {
                    if ((now - masterMember.getLastRead()) >= 10000) {
                        doRemoveAddress(getMasterAddress());
                        removed = true;
                    }
                }
                if (!removed) {
                    Packet packet = obtainPacket("heartbeat", null, null, OP_HEARTBEAT,
                            0);
                    Connection connMaster = ConnectionManager.get().getOrConnect(getMasterAddress());
                    sendOrReleasePacket(packet, connMaster);
                }
            }
            for (MemberImpl member : lsMembers) {
                if (!member.localMember()) {
                    Address address = member.getAddress();
                    if (shouldConnectTo(address)) {
                        Connection conn = ConnectionManager.get().getOrConnect(address);
                        if (conn != null) {
                            Packet packet = obtainPacket("heartbeat", null, null,
                                    OP_HEARTBEAT, 0);
                            sendOrReleasePacket(packet, conn);
                        }
                    } else {
                        Connection conn = ConnectionManager.get().getConnection(address);
                        if (conn != null && conn.live()) {
                            if ((now - member.getLastWrite()) > 1000) {
                                Packet packet = obtainPacket("heartbeat", null, null,
                                        OP_HEARTBEAT, 0);
                                sendOrReleasePacket(packet, conn);
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean shouldConnectTo(Address address) {
        if (!Node.get().joined())
            return true;
        return (lsMembers.indexOf(getMember(thisAddress)) > lsMembers.indexOf(getMember(address)));
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
        if (!Node.get().joined()) {
            Node.get().setMasterAddress(master.address);
            Connection connMaster = ConnectionManager.get().getOrConnect(master.address);
            if (connMaster != null) {
                sendJoinRequest(master.address);
            }
        }
    }

    public void handleAddRemoveConnection(AddRemoveConnection addRemoveConnection) {
        boolean add = addRemoveConnection.add;
        Address addressChanged = addRemoveConnection.address;
        if (add) { // Just connect to the new address if not connected already.
            if (!addressChanged.equals(thisAddress)) {
                ConnectionManager.get().getOrConnect(addressChanged);
            }
        } else { // Remove dead member
            addressChanged.setDead();
            final Address deadAddress = addressChanged;
            doRemoveAddress(deadAddress);
        } // end of REMOVE CONNECTION
    }

    void doRemoveAddress(Address deadAddress) {
        if (DEBUG) {
            log("Removing Address " + deadAddress);
        }
        if (deadAddress.equals(thisAddress))
            return;
        if (deadAddress.equals(getMasterAddress())) {
            if (Node.get().joined()) {
                MemberImpl newMaster = getNextMemberAfter(deadAddress, false, 1);
                if (newMaster != null)
                    Node.get().setMasterAddress(newMaster.getAddress());
                else
                    Node.get().setMasterAddress(null);
            } else {
                Node.get().setMasterAddress(null);
            }
            if (DEBUG) {
                log("Now Master " + Node.get().getMasterAddress());
            }
        }

        if (isMaster()) {
            setJoins.remove(new MemberInfo(deadAddress, 0));
        }

        lsMembersBefore.clear();
        for (MemberImpl member : lsMembers) {
            lsMembersBefore.add(member);
        }
        Connection conn = ConnectionManager.get().getConnection(deadAddress);
        if (conn != null) {
            ConnectionManager.get().remove(conn);
        }
        MemberImpl member = getMember(deadAddress);
        if (member != null) {
            removeMember(deadAddress);
        }
        BlockingQueueManager.get().syncForDead(deadAddress);
        ConcurrentMapManager.get().syncForDead(deadAddress);
        ListenerManager.get().syncForDead(deadAddress);
        TopicManager.get().syncForDead(deadAddress);

        Node.get().getClusterImpl().setMembers(lsMembers);

        // toArray will avoid CME as onDisconnect does remove the calls
        Object[] calls = mapCalls.values().toArray();
        for (Object call : calls) {
            ((Call) call).onDisconnect(deadAddress);
        }
        System.out.println(this);
    }

    public List<MemberImpl> getMembersBeforeSync() {
        return lsMembersBefore;
    }

    void handleJoinRequest(JoinRequest joinRequest) {
        logger.log(Level.FINEST, joinInProgress + " Handling " + joinRequest);
        if (getMember(joinRequest.address) != null)
            return;
        if (DEBUG) {
            // log("Handling  " + joinRequest);
        }
        Connection conn = joinRequest.getConnection();
        if (!Config.get().getJoin().getMulticastConfig().isEnabled()) {
            if (Node.get().getMasterAddress() != null && !isMaster()) {
                sendProcessableTo(new Master(Node.get().getMasterAddress()), conn);
            }
        }
        if (isMaster()) {
            Address newAddress = joinRequest.address;
            if (!joinInProgress) {
                MemberInfo newMemberInfo = new MemberInfo(newAddress, joinRequest.nodeType);
                if (setJoins.add(newMemberInfo)) {
                    sendProcessableTo(new Master(Node.get().getMasterAddress()), conn);
                    // sendAddRemoveToAllConns(newAddress);
                    timeToStartJoin = System.currentTimeMillis() + waitTimeBeforeJoin;
                } else {
                    if (System.currentTimeMillis() > timeToStartJoin) {
                        startJoin();
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("\n\nMembers [");
        sb.append(lsMembers.size());
        sb.append("] {");
        for (MemberImpl member : lsMembers) {
            sb.append("\n\t" + member);
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

    public void sendProcessableTo(RemotelyProcessable rp, Connection conn) {
        Data value = ThreadContext.get().toData(rp);
        Packet packet = obtainPacket();
        try {
            packet.set("remotelyProcess", OP_REMOTELY_PROCESS, null, value);
            boolean sent = send(packet, conn);
            if (!sent) {
                packet.returnToContainer();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void joinReset() {
        joinInProgress = false;
        setJoins.clear();
        timeToStartJoin = System.currentTimeMillis() + waitTimeBeforeJoin + 1000;
    }

    public class AsyncRemotelyObjectCallable extends TargetAwareOp {
        AbstractRemotelyCallable arp = null;

        public void executeProcess(Address address, AbstractRemotelyCallable arp) {
            this.arp = arp;
            super.target = address;
            doOp(OP_REMOTELY_OBJECT_CALLABLE, "call", null, arp, 0, -1, -1);
        }

        @Override
        public void doLocalOp() {
            Object result;
            try {
                result = arp.call();
                setResult(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
		public
        void setTarget() {
        }
    }

    public class AsyncRemotelyBooleanCallable extends BooleanOp {
        AbstractRemotelyCallable<Boolean> arp = null;

        public void executeProcess(Address address, AbstractRemotelyCallable<Boolean> arp) {
            this.arp = arp;
            super.target = address;
            doOp(OP_REMOTELY_BOOLEAN_CALLABLE, "call", null, arp, 0, -1, -1);
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

    public class ResponsiveRemoteProcess extends TargetAwareOp {
        AbstractRemotelyProcessable arp = null;

        public boolean executeProcess(Address address, AbstractRemotelyProcessable arp) {
            this.arp = arp;
            super.target = address;
            return booleanCall(OP_REMOTELY_PROCESS_AND_RESPOND, "exe", null, arp, 0, -1, -1);
        }

        @Override
        public void doLocalOp() {
            arp.process();
            setResult(Boolean.TRUE);
        }

        @Override
		public
        void setTarget() {
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

    void startJoin() {
        joinInProgress = true;
        final MembersUpdateCall membersUpdate = new MembersUpdateCall(lsMembers);
        if (setJoins != null && setJoins.size() > 0) {
            for (MemberInfo memberJoined : setJoins) {
                membersUpdate.addMemberInfo(memberJoined);
            }
        }

        executeLocally(new Runnable() {
            public void run() {
                List<MemberInfo> lsMemberInfos = membersUpdate.lsMemberInfos;
                List<AsyncRemotelyBooleanCallable> calls = new ArrayList<AsyncRemotelyBooleanCallable>();
                for (final MemberInfo memberInfo : lsMemberInfos) {
                    AsyncRemotelyBooleanCallable rrp = new AsyncRemotelyBooleanCallable();
                    rrp.executeProcess(memberInfo.address, membersUpdate);
                    calls.add(rrp);
                }
                for (AsyncRemotelyBooleanCallable call : calls) {
                    call.getResultAsBoolean();
                }
                calls.clear();
                for (final MemberInfo memberInfo : lsMemberInfos) {
                    AsyncRemotelyBooleanCallable call = new AsyncRemotelyBooleanCallable();
                    call.executeProcess(memberInfo.address, new SyncProcess());
                    calls.add(call);
                }
                for (AsyncRemotelyBooleanCallable call : calls) {
                    call.getResultAsBoolean();
                }
                calls.clear();
                AbstractRemotelyCallable<Boolean> connCheckcallable = new ConnectionCheckCall();
                for (final MemberInfo memberInfo : lsMemberInfos) {
                    AsyncRemotelyBooleanCallable call = new AsyncRemotelyBooleanCallable();
                    call.executeProcess(memberInfo.address, connCheckcallable);
                    calls.add(call);
                }
                for (AsyncRemotelyBooleanCallable call : calls) {
                    call.getResultAsBoolean();
                }
            }
        });
    }

    void updateMembers(List<MemberInfo> lsMemberInfos) {
        if (DEBUG) {
            log("MEMBERS UPDATE!!");
        }
        lsMembersBefore.clear();
        Map<Address, MemberImpl> mapOldMembers = new HashMap<Address, MemberImpl>();
        for (MemberImpl member : lsMembers) {
            lsMembersBefore.add(member);
            mapOldMembers.put(member.getAddress(), member);
        }
        lsMembers.clear();
        for (MemberInfo memberInfo : lsMemberInfos) {
            MemberImpl member = mapOldMembers.get(memberInfo.address);
            if (member == null) {
                member = addMember(memberInfo.address, memberInfo.nodeType);
            } else {
                addMember(member);
            }
            member.didRead();
        }
        mapOldMembers.clear();
        if (!lsMembers.contains(thisMember)) {
            throw new RuntimeException("Member list doesn't contain local member!");
        }
        mapMembers.clear();
        for (MemberImpl member : lsMembers) {
            mapMembers.put(member.getAddress(), member);
        }
        heartBeater();
        Node.get().getClusterImpl().setMembers(lsMembers);
        Node.get().unlock();
        logger.log(Level.INFO, this.toString());
    }

    public void sendJoinRequest(Address toAddress) {
        if (toAddress == null) {
            toAddress = Node.get().getMasterAddress();
        }
        sendProcessableTo(new JoinRequest(thisAddress, Config.get().getGroupName(),
                Config.get().getGroupPassword(), Node.get().getLocalNodeType()), toAddress);
    }


    public void sendBindRequest(Connection toConnection) {
        sendProcessableTo(new Bind(thisAddress), toConnection);
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

    public void connectionAdded(final Connection connection) {
        enqueueAndReturn(new Processable() {
            public void process() {
                MemberImpl member = getMember(connection.getEndPoint());
                if (member != null) {
                    member.didRead();
                }
            }
        });
    }

    public void connectionRemoved(Connection connection) {
    }

    public Member addMember(MemberImpl member) {
        if (DEBUG) {
            log("ClusterManager adding " + member);
        }
        if (lsMembers.contains(member)) {
            for (MemberImpl m : lsMembers) {
                if (m.equals(member))
                    member = m;
            }
        } else {
            if (!member.getAddress().equals(thisAddress)) {
                ConnectionManager.get().getConnection(member.getAddress());
            }
            lsMembers.add(member);
        }
        return member;
    }

    protected void removeMember(Address address) {
        if (DEBUG) {
            log("removing  " + address);
        }
        Member member = mapMembers.remove(address);
        if (member != null) {
            lsMembers.remove(member);
        }
    }

    protected MemberImpl createMember(Address address, int nodeType) {
        return new MemberImpl(address, thisAddress.equals(address), nodeType);
    }

    public MemberImpl getMember(Address address) {
        return mapMembers.get(address);
    }

    final public MemberImpl addMember(Address address, int nodeType) {
        if (address == null) {
            if (DEBUG) {
                log("Address cannot be null");
                return null;
            }
        }
        MemberImpl member = getMember(address);
        if (member == null)
            member = createMember(address, nodeType);
        addMember(member);
        return member;
    }

    public void stop() {
        setJoins.clear();
        timeToStartJoin = 0;
        lsMembers.clear();
    }

}
