package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
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
    public static final int MAXIMUM_CLAIM_SIZE = 5000;

    /**
     * If calculated claim size is smaller then this constant then we will
     * treat it as exhausted capacity and we return 0 slots. The purpose is to prevent issuing many small
     * claims as it would cause extra over-head.
     *
     */
    public static final int MINIMUM_CLAIM_SIZE = 10;

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
            newClaim = remainingCapacity / activeConnectionCount;
            if (newClaim >= MINIMUM_CLAIM_SIZE) {
                newClaim = Math.min(MAXIMUM_CAPACITY, newClaim);
            } else {
                newClaim = 0;
            }

            int reservedCapacityAfter = bookedCapacityWithoutMe + newClaim;
            if (bookedCapacity.compareAndSet(bookedCapacityBefore, reservedCapacityAfter)) {
                break;
            }
        }
        bookedCapacityPerMember.put(connection, newClaim);
        return newClaim;
    }

    private class ClaimCleaner implements ConnectionListener {

        @Override
        public void connectionAdded(Connection connection) {

        }

        @Override
        public void connectionRemoved(Connection connection) {
            Integer claim = bookedCapacityPerMember.get(connection);
            if (claim != null) {
                bookedCapacity.getAndAdd(-claim);
                bookedCapacityPerMember.remove(connection);
            }
        }
    }
}
