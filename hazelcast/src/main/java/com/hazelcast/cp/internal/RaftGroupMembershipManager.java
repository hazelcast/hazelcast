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

package com.hazelcast.cp.internal;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.MembershipChangeContext.CPGroupMembershipChangeContext;
import com.hazelcast.cp.internal.raft.MembershipChangeType;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.cp.internal.raftop.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.cp.internal.raftop.metadata.CompleteRaftGroupMembershipChangesOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.GetDestroyingRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetMembershipChangeContextOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.peel;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Realizes pending Raft group membership changes and periodically checks
 * validity of local {@link RaftNode} instances on CP members
 */
class RaftGroupMembershipManager {

    static final long MANAGEMENT_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    private static final long CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(10);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private volatile RaftInvocationManager invocationManager;

    RaftGroupMembershipManager(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
    }

    void init() {
        if (getLocalMember() == null) {
            return;
        }

        this.invocationManager = raftService.getInvocationManager();

        ExecutionService executionService = nodeEngine.getExecutionService();
        // scheduleWithRepetition skips subsequent execution if one is already running.
        executionService.scheduleWithRepetition(new RaftGroupDestroyHandlerTask(), MANAGEMENT_TASK_PERIOD_IN_MILLIS,
                MANAGEMENT_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new RaftGroupMembershipChangeHandlerTask(), MANAGEMENT_TASK_PERIOD_IN_MILLIS,
                MANAGEMENT_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new CheckLocalRaftNodesTask(), CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS,
                CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    private CPMemberInfo getLocalMember() {
        return raftService.getMetadataGroupManager().getLocalMember();
    }

    private boolean skipRunningTask() {
        return !raftService.getMetadataGroupManager().isMetadataGroupLeader();
    }

    private class CheckLocalRaftNodesTask implements Runnable {

        public void run() {
            for (RaftNode raftNode : raftService.getAllRaftNodes()) {
                final CPGroupId groupId = raftNode.getGroupId();
                if (groupId.equals(raftService.getMetadataGroupId())) {
                    continue;
                }

                if (raftNode.getStatus() == RaftNodeStatus.TERMINATED || raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                    raftService.destroyRaftNode(groupId);
                    continue;
                }

                ICompletableFuture<CPGroupInfo> f = queryMetadata(new GetRaftGroupOp(groupId));

                f.andThen(new ExecutionCallback<CPGroupInfo>() {
                    @Override
                    public void onResponse(CPGroupInfo group) {
                        if (group == null) {
                            logger.severe("Could not find CP group for local raft node of " + groupId);
                        } else if (group.status() == CPGroupStatus.DESTROYED) {
                            raftService.destroyRaftNode(groupId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warning("Could not get CP group info of " + groupId, t);
                    }
                });
            }
        }

    }

    private class RaftGroupDestroyHandlerTask implements Runnable {
        @Override
        public void run() {
            if (skipRunningTask()) {
                return;
            }

            Set<CPGroupId> destroyedGroupIds = destroyRaftGroups();
            if (destroyedGroupIds.isEmpty()) {
                return;
            }

            if (!commitDestroyedRaftGroups(destroyedGroupIds)) {
                return;
            }

            for (CPGroupId groupId : destroyedGroupIds) {
                raftService.destroyRaftNode(groupId);
            }

            OperationService operationService = nodeEngine.getOperationService();
            for (CPMemberInfo member : raftService.getMetadataGroupManager().getActiveMembers()) {
                if (!member.equals(raftService.getLocalMember())) {
                    operationService.send(new DestroyRaftNodesOp(destroyedGroupIds), member.getAddress());
                }
            }
        }

        private Set<CPGroupId> destroyRaftGroups() {
            Collection<CPGroupId> destroyingRaftGroupIds = getDestroyingRaftGroupIds();
            if (destroyingRaftGroupIds.isEmpty()) {
                return Collections.emptySet();
            }

            Map<CPGroupId, Future<Object>> futures = new HashMap<CPGroupId, Future<Object>>();
            for (CPGroupId groupId : destroyingRaftGroupIds) {
                Future<Object> future = invocationManager.destroy(groupId);
                futures.put(groupId, future);
            }

            Set<CPGroupId> destroyedGroupIds = new HashSet<CPGroupId>();
            for (Entry<CPGroupId, Future<Object>> e : futures.entrySet()) {
                if (isRaftGroupDestroyed(e.getKey(), e.getValue())) {
                    destroyedGroupIds.add(e.getKey());
                }
            }

            return destroyedGroupIds;
        }

        private Collection<CPGroupId> getDestroyingRaftGroupIds() {
            InternalCompletableFuture<Collection<CPGroupId>> f = queryMetadata(new GetDestroyingRaftGroupIdsOp());
            return f.join();
        }

        private boolean isRaftGroupDestroyed(CPGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (peel(e) instanceof CPGroupDestroyedException) {
                    return true;
                }

                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);

                return false;
            }
        }

        private boolean commitDestroyedRaftGroups(Set<CPGroupId> destroyedGroupIds) {
            RaftOp op = new CompleteDestroyRaftGroupsOp(destroyedGroupIds);
            CPGroupId metadataGroupId = raftService.getMetadataGroupId();
            Future<Collection<CPGroupId>> f = invocationManager.invoke(metadataGroupId, op);

            try {
                f.get();
                logger.info("Terminated CP groups: " + destroyedGroupIds + " are committed.");
                return true;
            } catch (Exception e) {
                logger.severe("Cannot commit terminated CP groups: " + destroyedGroupIds, e);
                return false;
            }
        }
    }

    private class RaftGroupMembershipChangeHandlerTask implements Runnable {

        private static final int NA_MEMBERS_COMMIT_INDEX = -1;

        @Override
        public void run() {
            if (skipRunningTask()) {
                return;
            }

            MembershipChangeContext membershipChangeContext = getMembershipChangeContext();
            if (membershipChangeContext == null) {
                return;
            }

            logger.fine("Handling " + membershipChangeContext);

            List<CPGroupMembershipChangeContext> changes = membershipChangeContext.getChanges();
            Map<CPGroupId, Tuple2<Long, Long>> changedGroups = new ConcurrentHashMap<CPGroupId, Tuple2<Long, Long>>();
            CountDownLatch latch = new CountDownLatch(changes.size());

            for (CPGroupMembershipChangeContext ctx : changes) {
                addMember(changedGroups, latch, ctx);
            }

            try {
                latch.await();
                completeMembershipChanges(changedGroups);
            } catch (InterruptedException e) {
                logger.warning("Membership changes interrupted while executing " + membershipChangeContext
                        + ". completed: " + changedGroups, e);
                Thread.currentThread().interrupt();
            }
        }

        private MembershipChangeContext getMembershipChangeContext() {
            InternalCompletableFuture<MembershipChangeContext> f = queryMetadata(new GetMembershipChangeContextOp());
            return f.join();
        }

        private void addMember(final Map<CPGroupId, Tuple2<Long, Long>> changedGroups,
                               final CountDownLatch latch,
                               final CPGroupMembershipChangeContext ctx) {
            final CPGroupId groupId = ctx.getGroupId();
            ICompletableFuture<Long> future;
            if (ctx.getMemberToAdd() == null) {
                future = newCompletedFuture(ctx.getMembersCommitIndex());
            } else {
                logger.fine("Adding " + ctx.getMemberToAdd() + " to " + groupId);

                future = invocationManager.changeMembership(groupId, ctx.getMembersCommitIndex(),
                        ctx.getMemberToAdd(), MembershipChangeType.ADD);
            }

            future.andThen(new ExecutionCallback<Long>() {
                @Override
                public void onResponse(Long addCommitIndex) {
                    if (ctx.getMemberToRemove() == null) {
                        changedGroups.put(groupId, Tuple2.of(ctx.getMembersCommitIndex(), addCommitIndex));
                        latch.countDown();
                    } else {
                        removeMember(changedGroups, latch, ctx, addCommitIndex);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    long addCommitIndex = getMemberAddCommitIndex(ctx, t);
                    if (addCommitIndex != NA_MEMBERS_COMMIT_INDEX) {
                        removeMember(changedGroups, latch, ctx, addCommitIndex);
                    } else {
                        latch.countDown();
                    }
                }
            });
        }

        private void removeMember(final Map<CPGroupId, Tuple2<Long, Long>> changedGroups,
                                  final CountDownLatch latch,
                                  final CPGroupMembershipChangeContext ctx,
                                  final long currentCommitIndex) {
            ICompletableFuture<Long> future = invocationManager.changeMembership(ctx.getGroupId(), currentCommitIndex,
                    ctx.getMemberToRemove(), MembershipChangeType.REMOVE);
            future.andThen(new ExecutionCallback<Long>() {
                @Override
                public void onResponse(Long removeCommitIndex) {
                    changedGroups.put(ctx.getGroupId(), Tuple2.of(ctx.getMembersCommitIndex(), removeCommitIndex));
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    long removeCommitIndex = getMemberRemoveCommitIndex(ctx, t);
                    if (removeCommitIndex != NA_MEMBERS_COMMIT_INDEX) {
                        changedGroups.put(ctx.getGroupId(), Tuple2.of(ctx.getMembersCommitIndex(), removeCommitIndex));
                    }
                    latch.countDown();
                }
            });
        }

        private ICompletableFuture<Long> newCompletedFuture(long idx) {
            Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
            ILogger logger = nodeEngine.getLogger(getClass());
            SimpleCompletableFuture<Long> f = new SimpleCompletableFuture<Long>(executor, logger);
            f.setResult(idx);
            return f;
        }

        private long getMemberAddCommitIndex(CPGroupMembershipChangeContext ctx, Throwable t) {
            if (t.getCause() instanceof MismatchingGroupMembersCommitIndexException) {
                MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) t.getCause();

                String msg = "MEMBER ADD commit of " + ctx.getMemberToAdd() + " to " + ctx.getGroupId()
                        + " with members commit index: " + ctx.getMembersCommitIndex() + " failed. Actual group members: "
                        + m.getMembers() + " with commit index: " + m.getCommitIndex();

                if (m.getMembers().size() != ctx.getMembers().size() + 1) {
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                // learnt group members must contain the added member and the current members I know

                if (!m.getMembers().contains(ctx.getMemberToAdd())) {
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                for (CPMemberInfo member : ctx.getMembers()) {
                    if (!m.getMembers().contains(member)) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }
                }

                return m.getCommitIndex();
            }

            logger.severe("Cannot get MEMBER ADD result of " + ctx.getMemberToAdd() + " to " + ctx.getGroupId()
                    + " with members commit index: " + ctx.getMembersCommitIndex(), t);
            return NA_MEMBERS_COMMIT_INDEX;
        }

        private long getMemberRemoveCommitIndex(CPGroupMembershipChangeContext ctx, Throwable t) {
            CPMemberInfo removedMember = ctx.getMemberToRemove();

            if (t.getCause() instanceof MismatchingGroupMembersCommitIndexException) {
                MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) t.getCause();

                String msg = "MEMBER REMOVE commit of " + removedMember + " to " + ctx.getGroupId()
                        + " failed. Actual group members: " + m.getMembers() + " with commit index: " + m.getCommitIndex();

                if (m.getMembers().contains(removedMember)) {
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                if (ctx.getMemberToAdd() != null) {
                    // I expect the added member to be joined to the group
                    if (!m.getMembers().contains(ctx.getMemberToAdd())) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    // I know the removed member has left the group and the added member has joined.
                    // So member sizes must be same...
                    if (m.getMembers().size() != ctx.getMembers().size()) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }
                } else if (m.getMembers().size() != (ctx.getMembers().size() - 1)) {
                    // if there is no added member, I expect number of the learnt group members to be 1 less than
                    // the current members I know
                    logger.severe(msg);
                    return NA_MEMBERS_COMMIT_INDEX;
                }

                for (CPMemberInfo member : ctx.getMembers()) {
                    // Other group members except the removed one and added one must be still present...
                    if (!member.equals(removedMember) && !m.getMembers().contains(member)) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }
                }

                return m.getCommitIndex();
            }

            logger.severe("Cannot get MEMBER REMOVE result of " + removedMember + " to " + ctx.getGroupId(), t);
            return NA_MEMBERS_COMMIT_INDEX;
        }

        private void completeMembershipChanges(Map<CPGroupId, Tuple2<Long, Long>> changedGroups) {
            RaftOp op = new CompleteRaftGroupMembershipChangesOp(changedGroups);
            CPGroupId metadataGroupId = raftService.getMetadataGroupId();
            ICompletableFuture<Object> future = invocationManager.invoke(metadataGroupId, op);

            try {
                future.get();
            } catch (Exception e) {
                logger.severe("Cannot commit CP group membership changes: " + changedGroups, e);
            }
        }
    }

    private <T> InternalCompletableFuture<T> queryMetadata(RaftOp raftOp) {
        return invocationManager.query(raftService.getMetadataGroupId(), raftOp, LEADER_LOCAL);
    }
}
