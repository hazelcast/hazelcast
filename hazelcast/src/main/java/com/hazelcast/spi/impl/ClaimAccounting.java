package com.hazelcast.spi.impl;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ClaimAccounting {
    public static final int MAXIMUM_CAPACITY = 1000000;
    public static final int MAXIMUM_CLAIM_SIZE = 1000;

    private final AtomicInteger bookedCapacity;
    private final ConcurrentMap<Connection, Integer> bookedCapacityPerMember;

    private final InternalOperationService internalOperationService;
    private final ConnectionManager connectionManager;

    public ClaimAccounting(InternalOperationService internalOperationService, ConnectionManager connectionManager) {
        this.internalOperationService = internalOperationService;
        this.connectionManager = connectionManager;
        bookedCapacityPerMember = new ConcurrentHashMap<Connection, Integer>();
        bookedCapacity = new AtomicInteger();
    }

    public int claimSlots(Connection connection) {
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
