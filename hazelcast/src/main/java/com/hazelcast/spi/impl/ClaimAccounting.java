package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for dealing with back-pressure claim requests.
 */
public class ClaimAccounting {
    public static final int MAXIMUM_CAPACITY = 1000000;
    public static final int MAXIMUM_CLAIM_SIZE = 1000;

    private final AtomicInteger bookedCapacity = new AtomicInteger();
    private final ConcurrentMap<Connection, Integer> bookedCapacityPerMember = new ConcurrentHashMap<Connection, Integer>();

    private final NodeEngine nodeEngine;
    private final Node node;

    public ClaimAccounting(NodeEngine nodeEngine, Node node) {
       this.nodeEngine = nodeEngine;
        this.node = node;
    }

    public int claimSlots(Connection connection) {
        InternalOperationService internalOperationService = (InternalOperationService) nodeEngine.getOperationService();
        ConnectionManager connectionManager = node.getConnectionManager();

        int newClaim;
        for ( ;; ) {
            int bookedCapacityBefore = bookedCapacity.get();
            Integer myClaimsBefore = bookedCapacityPerMember.get(connection);
            myClaimsBefore = myClaimsBefore != null ? myClaimsBefore : 0;

            int noOfScheduledOperations = internalOperationService.getNoOfScheduledOperations();
            int bookedCapacityWithoutMe = bookedCapacityBefore - myClaimsBefore;

            int remainingCapacity = MAXIMUM_CAPACITY - noOfScheduledOperations - bookedCapacityWithoutMe;
            int activeConnectionCount = connectionManager.getActiveConnectionCount();
            newClaim = Math.min(MAXIMUM_CLAIM_SIZE, remainingCapacity / activeConnectionCount);

            int reservedCapacityAfter = bookedCapacityWithoutMe + newClaim;
            if (bookedCapacity.compareAndSet(bookedCapacityBefore, reservedCapacityAfter)) {
                break;
            }
        }
        bookedCapacityPerMember.put(connection, newClaim);
        return newClaim;
    }
}
