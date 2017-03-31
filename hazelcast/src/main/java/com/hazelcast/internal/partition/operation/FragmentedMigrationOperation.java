package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.ReplicaFragmentMigrationState;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.ReplicaFragmentNamespace;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;

// TODO [basri] JAVADOC
// TODO [basri] eliminate duplication
public class FragmentedMigrationOperation extends BaseMigrationOperation {

    private static final OperationResponseHandler ERROR_RESPONSE_HANDLER = new OperationResponseHandler() {
        @Override
        public void sendResponse(Operation op, Object obj) {
            throw new HazelcastException("Migration operations can not send response!");
        }
    };

    private ReplicaFragmentMigrationState fragmentMigrationState;

    private Throwable failureReason;

    public FragmentedMigrationOperation() {
    }

    public FragmentedMigrationOperation(MigrationInfo migrationInfo, int partitionStateVersion,
                                        ReplicaFragmentMigrationState fragmentMigrationState) {
        super(migrationInfo, partitionStateVersion);
        this.fragmentMigrationState = fragmentMigrationState;
    }

    @Override
    public void run() throws Exception {
        checkMigrationInitiatorIsMaster();
        setActiveMigration();

        try {
            doRun();
        } catch (Throwable t) {
            logMigrationFailure(t);
            success = false;
            failureReason = t;
        } finally {
            onMigrationComplete();
            if (!success) {
                onExecutionFailure(failureReason);
            }
        }
    }

    private void doRun() throws Exception {
        if (migrationInfo.startProcessing()) {
            try {
                executeBeforeMigrations();

                for (Operation migrationOperation : fragmentMigrationState.getMigrationOperations()) {
                    runMigrationOperation(migrationOperation);
                }

                success = true;
            } catch (Throwable e) {
                success = false;
                failureReason = e;
                getLogger().severe("Error while executing replication operations " + migrationInfo, e);
            } finally {
                afterMigrate();
            }
        } else {
            success = false;
            logMigrationCancelled();
        }
    }

    private void runMigrationOperation(Operation op) throws Exception {
        prepareOperation(op);
        op.beforeRun();
        op.run();
        op.afterRun();
    }

    private void prepareOperation(Operation op) {
        op.setNodeEngine(getNodeEngine())
          .setPartitionId(getPartitionId())
          .setReplicaIndex(getReplicaIndex());
        op.setOperationResponseHandler(ERROR_RESPONSE_HANDLER);
        OperationAccessor.setCallerAddress(op, migrationInfo.getSource());
    }

    private void afterMigrate() {
        if (success) {
            InternalPartitionServiceImpl partitionService = getService();
            PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
            int destinationNewReplicaIndex = migrationInfo.getDestinationNewReplicaIndex();
            int replicaOffset = destinationNewReplicaIndex <= 1 ? 1 : destinationNewReplicaIndex;

            for (Map.Entry<ReplicaFragmentNamespace, long[]> e  : fragmentMigrationState.getNamespaces().entrySet()) {
                ReplicaFragmentNamespace namespace = e.getKey();
                long[] replicaVersions = e.getValue();
                replicaManager.setPartitionReplicaVersions(migrationInfo.getPartitionId(), namespace, replicaVersions, replicaOffset);
                if (getLogger().isFinestEnabled()) {
                    getLogger().finest("ReplicaVersions are set after migration. partitionId="
                            + migrationInfo.getPartitionId() + " replicaVersions=" + Arrays.toString(replicaVersions));
                }
            }
        } else if (getLogger().isFinestEnabled()) {
            getLogger().finest("ReplicaVersions are not set since migration failed. partitionId="
                    + migrationInfo.getPartitionId());
        }

        migrationInfo.doneProcessing();
    }

    private void logMigrationCancelled() {
        getLogger().warning("Migration is cancelled -> " + migrationInfo);
    }

    // TODO [basri] move this to BaseMigrationOperator
    private void checkMigrationInitiatorIsMaster() {
        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!masterAddress.equals(migrationInfo.getMaster())) {
            // TODO [basri] instead of retry, we should fail here.
            throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
        }
    }

    private void logMigrationFailure(Throwable e) {
        Level level = Level.WARNING;
        if (e instanceof IllegalStateException) {
            level = Level.FINEST;
        }
        ILogger logger = getLogger();
        if (logger.isLoggable(level)) {
            logger.log(level, e.getMessage(), e);
        }
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.FRAGMENTED_MIGRATION;
    }

    @Override
    void onMigrationStart() {
        // TODO [basri] call fragmented listeners here
    }

    @Override
    void onMigrationComplete(boolean result) {
        // TODO [basri] call fragmented listeners here
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.DESTINATION, migrationInfo.getPartitionId(),
                migrationInfo.getDestinationCurrentReplicaIndex(), migrationInfo.getDestinationNewReplicaIndex());
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.DESTINATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        fragmentMigrationState.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ReplicaFragmentMigrationState fragmentMigrationState = new ReplicaFragmentMigrationState();
        fragmentMigrationState.readData(in);
    }

}
