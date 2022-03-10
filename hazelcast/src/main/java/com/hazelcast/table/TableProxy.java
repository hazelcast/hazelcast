package com.hazelcast.table;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.nextgen.OpCodes;
import com.hazelcast.spi.impl.nextgen.Request;
import com.hazelcast.spi.impl.nextgen.OpService;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;
import com.hazelcast.table.impl.TableService;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public class TableProxy<K,V> extends AbstractDistributedObject implements Table<K,V> {

    private final OpService opService;
    private final String name;

    public TableProxy(NodeEngineImpl nodeEngine, TableService tableService, String name) {
        super(nodeEngine, tableService);
        this.opService = nodeEngine.getOpService();
        this.name = name;
    }

    @Override
    public void upsert(V item) {
        Request request = new Request();
        request.opcode = OpCodes.TABLE_UPSERT;
        request.partitionId = 0;
        CompletableFuture f = opService.invoke(request);
        f.join();
    }

    @Override
    public void selectByKey(K key) {
        Request request = new Request();
        request.opcode = OpCodes.TABLE_SELECT_BY_KEY;
        request.partitionId = 0;
        CompletableFuture f = opService.invoke(request);
        f.join();
    }

    @Override
    public String getName() {
        return name;
    }

    @NotNull
    @Override
    public DestroyEventContext getDestroyContextForTenant() {
        return super.getDestroyContextForTenant();
    }

    @Override
    public String getServiceName() {
        return TableService.SERVICE_NAME;
    }
}
