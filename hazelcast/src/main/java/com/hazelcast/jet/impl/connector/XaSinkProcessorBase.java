/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.processor.TransactionPoolSnapshotUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.transaction.impl.xa.SerializableXID;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.Util.idToString;
import static javax.transaction.xa.XAException.XA_RETRY;
import static javax.transaction.xa.XAResource.TMFAIL;
import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSUCCESS;
import static javax.transaction.xa.XAResource.XA_RDONLY;

/**
 * Base class for sinks writing to resources using the X/Open XA Transactions,
 * mapped by Java Transaction API (JTA).
 */
public abstract class XaSinkProcessorBase implements Processor {
    private static final int COMMIT_RETRY_DELAY_MS = 100;

    protected TransactionPoolSnapshotUtility<XaTransactionId, XaTransaction> snapshotUtility;
    private ProcessingGuarantee externalGuarantee;
    private Context context;
    private XAResource xaResource;

    protected XaSinkProcessorBase(ProcessingGuarantee externalGuarantee) {
        this.externalGuarantee = externalGuarantee;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        this.context = context;
        externalGuarantee = Util.min(externalGuarantee, context.processingGuarantee());
        snapshotUtility = new TransactionPoolSnapshotUtility<>(
                outbox,
                context,
                false,
                externalGuarantee,
                2,
                (procIndex, txnIndex) -> new XaTransactionId(context, procIndex, txnIndex),
                XaTransaction::new,
                this::recoverTransaction,
                this::abortTransaction);
    }

    @Override
    public boolean tryProcess() {
        return snapshotUtility.tryProcess();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        // sinks can ignore the watermark
        return true;
    }

    @Override
    public boolean complete() {
        snapshotUtility.afterCompleted();
        return true;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return snapshotUtility.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        return snapshotUtility.snapshotCommitFinish(success);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        snapshotUtility.restoreFromSnapshot(inbox);
    }

    @Override
    public void close() throws Exception {
        if (snapshotUtility != null) {
            snapshotUtility.close();
        }
    }

    @Override
    public boolean isCooperative() {
        // at least the `commit` call isn't cooperative
        return false;
    }

    /**
     * Call this method from {@link Processor#init} if {@link
     * #snapshotUtility}{@code .usesTransactionLifecycle()} returns {@code
     * true}.
     */
    public void setXaResource(XAResource xaResource) {
        this.xaResource = xaResource;
        if (snapshotUtility.usesTransactionLifecycle() && xaResource == null) {
            throw new JetException("null XA resource set where it was required");
        }
    }

    private void recoverTransaction(Xid xid) throws InterruptedException {
        if (!snapshotUtility.usesTransactionLifecycle()) {
            return;
        }
        for (;;) {
            try {
                xaResource.commit(xid, false);
                context.logger().info("Successfully committed restored transaction ID: " + xid);
            } catch (XAException e) {
                switch (e.errorCode) {
                    case XA_RETRY:
                        LoggingUtil.logFine(context.logger(), "Commit failed with XA_RETRY, will retry in %s ms. XID: %s",
                                COMMIT_RETRY_DELAY_MS, xid);
                        Thread.sleep(COMMIT_RETRY_DELAY_MS);
                        LoggingUtil.logFine(context.logger(), "Retrying commit %s", xid);
                        continue;
                    case XAException.XA_HEURCOM:
                        context.logger().info("Due to a heuristic decision, the work done on behalf of " +
                                "the specified transaction branch was already committed. Transaction ID: " + xid);
                        break;
                    case XAException.XA_HEURRB:
                        context.logger().warning("Due to a heuristic decision, the work done on behalf of the restored " +
                                        "transaction ID was rolled back. Messages written in that transaction are lost. " +
                                        "Ignoring the problem and will continue the job. Transaction ID: " + xid,
                                handleXAException(e, xid));
                        break;
                    case XAException.XAER_NOTA:
                        LoggingUtil.logFine(context.logger(), "Failed to commit XID restored from snapshot: The " +
                                "specified XID is not known to the resource manager. This happens normally when the " +
                                "transaction was committed in phase 2 of the snapshot and can be ignored, but can " +
                                "happen also if the transaction wasn't committed in phase 2 and the RM lost it (in " +
                                "this case data written in it is lost). Transaction ID: %s", xid);
                        break;
                    default:
                        throw new JetException("Failed to commit XID restored from the snapshot, XA error code: " +
                                e.errorCode + ". Data loss is possible. Transaction ID: " + xid + ", cause: " + e,
                                handleXAException(e, xid));
                }
            }
            break;
        }
    }

    private void abortTransaction(Xid xid) {
        if (!snapshotUtility.usesTransactionLifecycle()) {
            return;
        }
        try {
            xaResource.rollback(xid);
        } catch (XAException e) {
            // We ignore rollback failures.
            // If error is XAER_NOTA (transaction doesn't exist), we don't even log it, this is the normal
            // case, we roll back preemptively
            if (e.errorCode != XAException.XAER_NOTA) {
                LoggingUtil.logFine(context.logger(), "Failed to roll back, transaction ID: %s. Error: %s",
                        xid, handleXAException(e, xid));
            }
        }
    }

    private XAException handleXAException(XAException e, Xid xid) {
        if (e.getMessage() == null) {
            // workaround for https://issues.apache.org/jira/browse/ARTEMIS-2532
            // the exception has `errorCode` field, but no message, let's throw a new one with errorCode in the message
            return new BetterXAException("errorCode=" + e.errorCode + (xid != null ? ", xid=" + xid : ""), e.errorCode, e);
        }
        return e;
    }

    private final class XaTransaction implements TransactionalResource<XaTransactionId> {
        private final XaTransactionId xid;
        private boolean ignoreCommit;
        private boolean isAssociated;

        private XaTransaction(XaTransactionId xid) {
            this.xid = xid;
        }

        @Override
        public XaTransactionId id() {
            return xid;
        }

        @Override
        public void begin() throws XAException {
            assert !isAssociated : "already associated";
            try {
                xaResource.start(xid, TMNOFLAGS);
                isAssociated = true;
            } catch (XAException e) {
                throw handleXAException(e, xid);
            }
        }

        @Override
        public void endAndPrepare() throws XAException {
            assert isAssociated : "not associated";
            try {
                xaResource.end(xid, TMSUCCESS);
                isAssociated = false;
                int res = xaResource.prepare(xid);
                ignoreCommit = res == XA_RDONLY;
            } catch (XAException e) {
                throw handleXAException(e, xid);
            }
        }

        @Override
        public void commit() throws XAException {
            if (!ignoreCommit) {
                try {
                    xaResource.commit(xid, false);
                } catch (XAException e) {
                    throw handleXAException(e, xid);
                }
            }
        }

        @Override
        public void rollback() throws XAException {
            assert isAssociated : "not associated";
            try {
                xaResource.end(xid, TMFAIL);
                isAssociated = false;
                xaResource.rollback(xid);
            } catch (XAException e) {
                throw handleXAException(e, xid);
            }
        }

        @Override
        public void release() throws Exception {
            if (isAssociated) {
                xaResource.end(xid, TMSUCCESS);
            }
        }
    }

    @BinaryInterface
    private static final class XaTransactionId extends SerializableXID implements TransactionId {

        /**
         * Number generated out of thin air.
         */
        private static final int JET_FORMAT_ID = 275827911;

        private static final int OFFSET_JOB_ID = 0;
        private static final int OFFSET_JOB_NAME_HASH = OFFSET_JOB_ID + Bits.LONG_SIZE_IN_BYTES;
        private static final int OFFSET_VERTEX_ID_HASH = OFFSET_JOB_NAME_HASH + Bits.LONG_SIZE_IN_BYTES;
        private static final int OFFSET_PROCESSOR_INDEX = OFFSET_VERTEX_ID_HASH + Bits.LONG_SIZE_IN_BYTES;
        private static final int OFFSET_TRANSACTION_INDEX = OFFSET_PROCESSOR_INDEX + Bits.INT_SIZE_IN_BYTES;
        private static final int GTRID_LENGTH = OFFSET_TRANSACTION_INDEX + Bits.INT_SIZE_IN_BYTES;

        @SuppressWarnings("unused") // needed for deserialization
        private XaTransactionId() {
        }

        private XaTransactionId(Processor.Context context, int processorIndex, int transactionIndex) {
            super(JET_FORMAT_ID,
                    createGtrid(context.jobId(),
                            stringHash(context.jobConfig().getName()),
                            stringHash(context.vertexName()),
                            processorIndex, transactionIndex),
                    new byte[1]);
        }

        private static long stringHash(String string) {
            byte[] bytes = String.valueOf(string).getBytes(StandardCharsets.UTF_8);
            return HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
        }

        private static byte[] createGtrid(
                long jobId,
                long jobNameHash,
                long vertexIdHash,
                int processorIndex,
                int transactionIndex
        ) {
            byte[] res = new byte[GTRID_LENGTH];
            Bits.writeLong(res, OFFSET_JOB_ID, jobId, true);
            Bits.writeLong(res, OFFSET_JOB_NAME_HASH, jobNameHash, true);
            Bits.writeLong(res, OFFSET_VERTEX_ID_HASH, vertexIdHash, true);
            Bits.writeInt(res, OFFSET_PROCESSOR_INDEX, processorIndex, true);
            Bits.writeInt(res, OFFSET_TRANSACTION_INDEX, transactionIndex, true);
            return res;
        }

        @Override
        public int index() {
            return Bits.readInt(getGlobalTransactionId(), OFFSET_PROCESSOR_INDEX, true);
        }

        @Override
        public String toString() {
            return XaTransactionId.class.getSimpleName() + "{"
                    + "jobId=" + idToString(Bits.readLong(getGlobalTransactionId(), OFFSET_JOB_ID, true)) + ", "
                    + "jobNameHash=" + Bits.readLong(getGlobalTransactionId(), OFFSET_JOB_NAME_HASH, true) + ", "
                    + "vertexIdHash=" + Bits.readLong(getGlobalTransactionId(), OFFSET_VERTEX_ID_HASH, true) + ", "
                    + "processorIndex=" + index() + ", "
                    + "transactionIndex=" + Bits.readInt(getGlobalTransactionId(), OFFSET_TRANSACTION_INDEX, true) + "}";
        }
    }

    private static final class BetterXAException extends XAException {

        private static final long serialVersionUID = 1L;

        private BetterXAException(String message, int errorCode, Throwable cause) {
            super(message);
            initCause(cause);
            this.errorCode = errorCode;
        }
    }
}
