package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.NodeEngine;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Root schema.
 */
public class HazelcastRootSchema extends AbstractSchema {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Table map. */
    private Map<String, Table> tableMap;

    public HazelcastRootSchema(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> res = tableMap;

        if (res == null) {
            res = getTableMap0();

            tableMap = res;
        }

        return res;
    }

    private Map<String, Table> getTableMap0() {
        Collection<String> mapNames = nodeEngine.getProxyService().getDistributedObjectNames(MapService.SERVICE_NAME);

        Map<String, Table> res = new HashMap<>();

        for (String mapName : mapNames) {
            Object map = nodeEngine.getProxyService().getDistributedObject(MapService.SERVICE_NAME, mapName);

            HazelcastTable table = new HazelcastTable(nodeEngine, map);

            res.put(mapName, table);
        }

        return res;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
