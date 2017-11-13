package com.hazelcast.raft;

/**
 * Policy to query committed Raft state without appending a log entry but in some cases giving up
 * strong consistency guarantees.
 */
public enum QueryPolicy {

    /**
     * Query committed Raft state locally only on Raft group leader.
     * <p>
     * If the leader is split from the group and a new leader is elected already,
     * stale values can be read until it realizes the split.
     * <p>
     * This policy is likely to hit more recent data when compared to {@link #ANY_LOCAL}.
     */
    LEADER_LOCAL,

    /**
     * Query committed Raft state locally on any Raft group member.
     * <p>
     * Reading stale value is possible when a follower lags behind the leader or is split from rest of the group.
     * <p>
     * {@link #LEADER_LOCAL} should be preferred if it's important to read up-to-date data mostly.
     */
    ANY_LOCAL,

    /**
     * Query Raft state in a linearizable manner, either by appending a log entry to Raft log
     * or using the algorithm defined in <i>6.4 Processing read-only queries more efficiently</i>
     * section of Raft dissertation.
     */
    LINEARIZABLE
}
