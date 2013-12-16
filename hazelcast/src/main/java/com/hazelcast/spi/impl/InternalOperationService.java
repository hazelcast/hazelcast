package com.hazelcast.spi.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

/**
 * This is the interface that needs to be implemented by actual InternalOperationService. Currently there is a single
 * InternalOperationService: {@link com.hazelcast.spi.impl.BasicOperationService}, but in the future others can be added.
 *
 * It exposes methods that will not be called by regular code, like shutdown, but will only be called by
 * the the SPI management.
 */
public interface InternalOperationService extends OperationService{

    void handleOperation(Packet packet);

    void onMemberLeft(MemberImpl member);

    void shutdown();

    void notifyBackupCall(long callId);

    void notifyRemoteCall(long callId, Object response);

    boolean isCallTimedOut(Operation op);
}
