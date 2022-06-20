package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.map.impl.operation.TSPutBackupOperation.lock;
import static com.hazelcast.map.impl.operation.TSPutBackupOperation.unlock;

public class TSRemoveBackupOperation extends RemoveBackupOperation
    implements BlockingOperation, Notifier {

    private transient Record status;

    private UUID ownerUuid;

    public TSRemoveBackupOperation(String name, Data dataKey,
                                 boolean disableWanReplicationEvent) {
        super(name, dataKey, disableWanReplicationEvent);
        this.ownerUuid = UuidUtil.newUnsecureUUID();
    }

    public TSRemoveBackupOperation() {
        super();
    }

    @Override
    protected void runInternal() {
        long finalThreadId = getThreadId();
        long finalCallId = getCallId();

        if (!recordStore.isLockedBy(dataKey, ownerUuid, finalThreadId)) {
            lock(recordStore, dataKey, ownerUuid, finalThreadId, finalCallId);
        }

        Record status = recordStore.removeBackup(dataKey, getCallerProvenance());
        if (isPendingIO(status)) {
            this.status = status;
            return;
        }

        unlock(recordStore, dataKey, ownerUuid, finalThreadId, finalCallId);
    }

    @Override
    public boolean isPendingResult() {
        return isPendingIO(status);
    }

    @Override
    protected Offload newIOOperationOffload() {
        return recordStore.newIOOperationOffload(dataKey, this, status);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    public boolean shouldWait() {
        if (recordStore.canAcquireLock(dataKey, ownerUuid, threadId)
            || recordStore.isLockedBy(dataKey, ownerUuid, threadId)) {
            // Don't wait if there is no any lock on dataKey or the operation already acquired it
            return false;
        }

        return true;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public boolean shouldNotify() {
        if (recordStore.canAcquireLock(dataKey, ownerUuid, threadId)) {
            // The operation released the lock
            return true;
        }
        return false;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TS_REMOVE_BACKUP;
    }

}
