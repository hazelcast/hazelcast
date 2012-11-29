package com.hazelcast.queue;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.queue.proxy.DataQueueProxy;
import com.hazelcast.queue.proxy.ObjectQueueProxy;
import com.hazelcast.queue.proxy.QueueProxy;
import com.hazelcast.spi.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:21 AM
 */
public class QueueService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService {

    private NodeService nodeService;

    public static final String NAME = "hz:impl:queueService";

    private final ILogger logger;

    private final ConcurrentMap<String, QueueContainer> containerMap = new ConcurrentHashMap<String, QueueContainer>();
    private final ConcurrentMap<String, QueueProxy> proxies = new ConcurrentHashMap<String, QueueProxy>();

    public QueueService(NodeService nodeService) {
        this.nodeService = nodeService;
        this.logger = nodeService.getLogger(QueueService.class.getName());
    }

    public Queue<Data> getQueue(final String name) {
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            container = new QueueContainer(nodeService.getPartitionId(nodeService.toData(name)));
            containerMap.put(name, container);
        }
        return container.dataQueue;
    }

    public void addContainer(String name, QueueContainer container) {
        containerMap.put(name, container);
    }


    public void init(NodeService nodeService, Properties properties) {
        this.nodeService = nodeService;
    }

    public void destroy() {
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
        // TODO: what if partition has transactions? what if not?
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        if (event.getPartitionId() < 0 || event.getPartitionId() >= nodeService.getPartitionCount()) {
            return null; // is it possible
        }
        Map<String, QueueContainer> migrationData = new HashMap<String, QueueContainer>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
            if (container.partitionId == event.getPartitionId()) {
                migrationData.put(name, container);
            }
        }
        return new QueueMigrationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    public void commitMigration(MigrationServiceEvent event) {
        logger.log(Level.FINEST, "commit " + event.getPartitionId());
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE
                && event.getMigrationType() == MigrationType.MOVE) {
            cleanMigrationData(event.getPartitionId());
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            cleanMigrationData(event.getPartitionId());
        }
    }

    private void cleanMigrationData(int partitionId) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getValue().partitionId == partitionId) {
                iterator.remove();
            }
        }
    }

    public void memberAdded(MemberImpl member) {
    }

    public void memberRemoved(MemberImpl member) {
    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            return new DataQueueProxy(name, this, nodeService);
        }
        final QueueProxy proxy = new ObjectQueueProxy(name, this, nodeService);
        final QueueProxy currentProxy = proxies.putIfAbsent(name, proxy);
        return currentProxy != null ? currentProxy : proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }
}
