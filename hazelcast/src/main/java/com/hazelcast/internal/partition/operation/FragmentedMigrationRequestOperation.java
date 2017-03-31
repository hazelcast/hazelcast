package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.DefaultReplicaFragmentNamespace;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.ReplicaFragmentMigrationState;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ReplicaFragmentNamespace;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import static com.hazelcast.internal.partition.ReplicaFragmentMigrationState.newDefaultReplicaFragmentMigrationState;
import static com.hazelcast.internal.partition.ReplicaFragmentMigrationState.newGroupedReplicaFragmentMigrationState;

public class FragmentedMigrationRequestOperation extends BaseMigrationOperation  {


    private boolean returnResponse = true;

    private ConcurrentMap<ReplicaFragmentNamespace, Boolean> allMigratingNamespaces = new ConcurrentHashMap<ReplicaFragmentNamespace, Boolean>();

    private volatile Collection<ReplicaFragmentNamespace> migratingNamespaces;

    public FragmentedMigrationRequestOperation() {
    }

    public FragmentedMigrationRequestOperation(MigrationInfo migrationInfo, int partitionStateVersion) {
        super(migrationInfo, partitionStateVersion);
    }

    @Override
    public void run() {
        NodeEngine nodeEngine = getNodeEngine();
        verifyGoodMaster(nodeEngine);

        Address source = migrationInfo.getSource();
        Address destination = migrationInfo.getDestination();
        verifyExistingTarget(nodeEngine, destination);

        if (destination.equals(source)) {
            getLogger().warning("Source and destination addresses are the same! => " + toString());
            setFailed();
            return;
        }

        InternalPartition partition = getPartition();
        verifySource(nodeEngine.getThisAddress(), partition);

        setActiveMigration();

        if (!migrationInfo.startProcessing()) {
            getLogger().warning("Migration is cancelled -> " + migrationInfo);
            setFailed();
            return;
        }

        try {
            executeBeforeMigrations();
            populateNamespaces();
            ReplicaFragmentMigrationState migrationState = createReplicaFragmentMigrationState();
            migratingNamespaces = migrationState.getNamespaces().keySet();
            invokeMigrationOperation(destination, migrationState);
            returnResponse = false;
        } catch (Throwable e) {
            logThrowable(e);
            setFailed();
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    private void setFailed() {
        success = false;
        onMigrationComplete(false);
    }

    private void logThrowable(Throwable t) {
        Throwable throwableToLog = t;
        if (throwableToLog instanceof ExecutionException) {
            throwableToLog = throwableToLog.getCause() != null ? throwableToLog.getCause() : throwableToLog;
        }
        Level level = getLogLevel(throwableToLog);
        getLogger().log(level, throwableToLog.getMessage(), throwableToLog);
    }

    private Level getLogLevel(Throwable e) {
        return (e instanceof MemberLeftException || e instanceof InterruptedException)
                || !getNodeEngine().isRunning() ? Level.INFO : Level.WARNING;
    }

    private void verifySource(Address thisAddress, InternalPartition partition) {
        Address owner = partition.getOwnerOrNull();
        if (owner == null) {
            throw new RetryableHazelcastException("Cannot migrate at the moment! Owner of the partition is null => "
                    + migrationInfo);
        }

        if (!thisAddress.equals(owner)) {
            throw new RetryableHazelcastException("Owner of partition is not this node! => " + toString());
        }
    }

    private void invokeMigrationOperation(Address destination, ReplicaFragmentMigrationState migrationState)
            throws IOException {
        Operation operation = new FragmentedMigrationOperation(migrationInfo, partitionStateVersion, migrationState);

        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();

        nodeEngine.getOperationService()
                  .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, operation, destination)
                  .setExecutionCallback(new MigrationCallback(migrationInfo, this))
                  .setResultDeserialized(true)
                  .setCallTimeout(partitionService.getPartitionMigrationTimeout())
                  .setTryCount(InternalPartitionService.MIGRATION_RETRY_COUNT)
                  .setTryPauseMillis(InternalPartitionService.MIGRATION_RETRY_PAUSE)
                  .setReplicaIndex(getReplicaIndex())
                  .invoke();
    }

    // TODO [basri] instead of retry, we should fail here.
    private void verifyGoodMaster(NodeEngine nodeEngine) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!migrationInfo.getMaster().equals(masterAddress)) {
            throw new RetryableHazelcastException("Migration initiator is not master node! => " + toString());
        }
        if (!masterAddress.equals(getCallerAddress())) {
            throw new RetryableHazelcastException("Caller is not master node! => " + toString());
        }
    }

    private void verifyExistingTarget(NodeEngine nodeEngine, Address destination) {
        Member target = nodeEngine.getClusterService().getMember(destination);
        if (target == null) {
            throw new TargetNotMemberException("Destination of migration could not be found! => " + toString());
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public boolean returnsResponse() {
        return returnResponse;
    }

    private void handleMigrationResultFromTarget(Object result) {
        if (Boolean.TRUE.equals(result)) {
            for (ReplicaFragmentNamespace namespace : migratingNamespaces) {
                allMigratingNamespaces.put(namespace, true);
            }
            migratingNamespaces = null;

            InternalOperationService operationService = (InternalOperationService) getNodeEngine().getOperationService();
            operationService.execute(new SendNewMigrationFragmentRunnable());
        } else {
            completeMigration(false);
        }
    }

    private void trySendNewFragment() {
        try {
            NodeEngine nodeEngine = getNodeEngine();
            verifyGoodMaster(nodeEngine);

            Address destination = migrationInfo.getDestination();
            verifyExistingTarget(nodeEngine, destination);

            InternalPartitionServiceImpl partitionService = getService();
            MigrationManager migrationManager = partitionService.getMigrationManager();
            MigrationInfo currentActiveMigration = migrationManager.setActiveMigration(migrationInfo);
            if (!migrationInfo.equals(currentActiveMigration)) {
                throw new IllegalStateException("Current active migration " + currentActiveMigration
                        + " is different than expected: " + migrationInfo);
            }

            ReplicaFragmentMigrationState migrationState = createReplicaFragmentMigrationState();
            if (migrationState != null) {
                migratingNamespaces = migrationState.getNamespaces().keySet();
                invokeMigrationOperation(destination, migrationState);
            } else {
                completeMigration(true);
            }
        } catch (Throwable e) {
            logThrowable(e);
            completeMigration(false);
        }
    }

    private void completeMigration(boolean result) {
        migrationInfo.doneProcessing();
        onMigrationComplete(result);
        sendResponse(result);
    }

    @Override
    void executeBeforeMigrations() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        boolean ownerMigration = nodeEngine.getThisAddress().equals(migrationInfo.getSource());
        if (!ownerMigration) {
            return;
        }

        super.executeBeforeMigrations();
    }

    private void populateNamespaces() {
        allMigratingNamespaces.put(DefaultReplicaFragmentNamespace.INSTANCE, false);
        for (Collection<ReplicaFragmentNamespace> serviceNamespaces : getAllReplicaFragmentNamespaces().values()) {
            for (ReplicaFragmentNamespace namespace : serviceNamespaces) {
                allMigratingNamespaces.put(namespace, false);
            }
        }
    }

    private ReplicaFragmentMigrationState createReplicaFragmentMigrationState() {
        if (allMigratingNamespaces.get(DefaultReplicaFragmentNamespace.INSTANCE)) {
            return createGroupedReplicaFragmentMigrationState();
        } else {
            return createDefaultReplicaFragmentMigrationState();
        }
    }

    private ReplicaFragmentMigrationState createDefaultReplicaFragmentMigrationState() {
        Collection<Operation> operations = new ArrayList<Operation>();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);
        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());

        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = (MigrationAwareService) serviceInfo.getService();
            if (service instanceof FragmentedMigrationAwareService) {
                continue;
            }

            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }

        int partitionId = getPartitionId();
        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        long[] versions = versionManager.getPartitionReplicaVersions(partitionId, DefaultReplicaFragmentNamespace.INSTANCE);

        return newDefaultReplicaFragmentMigrationState(versions, operations);
    }

    private ReplicaFragmentMigrationState createGroupedReplicaFragmentMigrationState() {
        Map<String, Collection<ReplicaFragmentNamespace>> allNamespaces = getAllReplicaFragmentNamespaces();

        Iterator<Collection<ReplicaFragmentNamespace>> serviceIt = allNamespaces.values().iterator();
        while (serviceIt.hasNext()) {
            Collection<ReplicaFragmentNamespace> namespaces = serviceIt.next();
            Iterator<ReplicaFragmentNamespace> namespaceIt = namespaces.iterator();
            while (namespaceIt.hasNext()) {
                ReplicaFragmentNamespace namespace = namespaceIt.next();
                if (!allMigratingNamespaces.containsKey(namespace) || allMigratingNamespaces.get(namespace)) {
                    namespaceIt.remove();
                }
            }

            if (namespaces.isEmpty()) {
                serviceIt.remove();
            }
        }

        if (allNamespaces.isEmpty()) {
            return null;
        }

        Entry<String, Collection<ReplicaFragmentNamespace>> e = allNamespaces.entrySet().iterator().next();
        String serviceName = e.getKey();
        Collection<ReplicaFragmentNamespace> namespaces = e.getValue();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
        PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());

        Operation op = service.prepareReplicationOperation(event, namespaces);
        if (op != null) {
            op.setServiceName(serviceName);

            InternalPartitionService partitionService = getService();
            PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
            Map<ReplicaFragmentNamespace, long[]> versions = new HashMap<ReplicaFragmentNamespace, long[]>();
            for (ReplicaFragmentNamespace namespace : namespaces) {
                long[] v = versionManager.getPartitionReplicaVersions(getPartitionId(), namespace);
                versions.put(namespace, v);
            }

            return newGroupedReplicaFragmentMigrationState(versions, op);
        }

        return null;
    }

    private Map<String, Collection<ReplicaFragmentNamespace>> getAllReplicaFragmentNamespaces() {
        Map<String, Collection<ReplicaFragmentNamespace>> namespaces = new HashMap<String, Collection<ReplicaFragmentNamespace>>();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);
        PartitionReplicationEvent replicationEvent = getPartitionReplicationEvent();

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = (FragmentedMigrationAwareService) serviceInfo.getService();
            Collection<ReplicaFragmentNamespace> allFragmentNamespaces = service.getAllFragmentNamespaces(replicationEvent);
            namespaces.put(serviceInfo.getName(), allFragmentNamespaces);
        }

        return namespaces;
    }

    private PartitionReplicationEvent getPartitionReplicationEvent() {
        return new PartitionReplicationEvent(migrationInfo.getPartitionId(),
                migrationInfo.getDestinationNewReplicaIndex());
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.FRAGMENTED_MIGRATION_REQUEST;
    }

    @Override
    protected PartitionMigrationEvent getMigrationEvent() {
        return new PartitionMigrationEvent(MigrationEndpoint.SOURCE, migrationInfo.getPartitionId(),
                migrationInfo.getSourceCurrentReplicaIndex(), migrationInfo.getSourceNewReplicaIndex());
    }

    @Override
    protected MigrationParticipant getMigrationParticipantType() {
        return MigrationParticipant.SOURCE;
    }

    private static final class MigrationCallback extends SimpleExecutionCallback<Object> {

        final MigrationInfo migrationInfo;
        final FragmentedMigrationRequestOperation op;

        private MigrationCallback(MigrationInfo migrationInfo, FragmentedMigrationRequestOperation op) {
            this.migrationInfo = migrationInfo;
            this.op = op;
        }

        @Override
        public void notify(Object result) {
            op.handleMigrationResultFromTarget(result);
        }
    }

    private final class SendNewMigrationFragmentRunnable implements PartitionSpecificRunnable {

        @Override
        public int getPartitionId() {
            return FragmentedMigrationRequestOperation.this.getPartitionId();
        }

        @Override
        public void run() {
            trySendNewFragment();
        }

    }

}
