package com.hazelcast.raft.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.spi.TaskScheduler;

import java.util.concurrent.Executor;

/**
 * TODO: Javadoc Pending...
 */
public interface RaftIntegration {

    TaskScheduler getTaskScheduler();

    Executor getExecutor();

    ILogger getLogger(String name);

    ILogger getLogger(Class clazz);

    boolean isJoined();

    boolean isReachable(RaftEndpoint endpoint);

    boolean send(VoteRequest request, RaftEndpoint target);

    boolean send(VoteResponse response, RaftEndpoint target);

    boolean send(AppendRequest request, RaftEndpoint target);

    boolean send(AppendSuccessResponse response, RaftEndpoint target);

    boolean send(AppendFailureResponse response, RaftEndpoint target);

    Object runOperation(RaftOperation operation, int commitIndex);

}
