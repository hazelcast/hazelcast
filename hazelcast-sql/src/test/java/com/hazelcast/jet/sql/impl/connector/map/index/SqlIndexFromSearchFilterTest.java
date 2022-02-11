package com.hazelcast.jet.sql.impl.connector.map.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.TablesStorage;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.createBiClass;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.createBiValue;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexFromSearchFilterTest extends SqlIndexTestSupport {
    private NodeEngine nodeEngine;
    private TableResolver resolver;

    private String mapName;
    private String indexName;

    private IMap<Integer, ? super ExpressionBiValue> map;
    private Class<? extends ExpressionBiValue> valueClass;
    private ExpressionBiValue value;

    @BeforeClass
    public static void beforeClass() {
        initialize(3, null);
    }

    @Before
    public void before() throws Exception {
        nodeEngine = getNodeEngine(instance());
        resolver = new TableResolverImpl(nodeEngine, new TablesStorage(nodeEngine), new SqlConnectorCache(nodeEngine));

        mapName = randomName();
        indexName = randomName();
        String[] indexAttributes = new String[]{"field1"};

        valueClass = createBiClass(INTEGER, INTEGER);
        value = createBiValue(valueClass, 1, 1, null);
        map = instance().getMap(mapName);

        createMapping(mapName, int.class, valueClass);
        createIndex(indexName, mapName, IndexType.SORTED, indexAttributes);

        for (int i = 1; i <= 100; ++i) {
            map.put(i, createBiValue(valueClass, i, i, i));
        }
    }

    @Test
    public void testSimpleRange() {
        String sql = "SELECT * FROM  \n" + mapName +
                " WHERE field1 >= -1\n" +
                " AND field1 <= 1 \n";
        checkIndexUsage(new SqlStatement(sql));
    }

    @Test
    public void testMultipleEquals() {
        String sql = "SELECT * FROM  \n" + mapName +
                " WHERE field1 = -1\n" +
                " OR field1 = 1 \n" +
                " OR field1 = 3 \n";
        checkIndexUsage(new SqlStatement(sql));
    }

    private void checkIndexUsage(SqlStatement statement) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.OBJECT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("field1", INTEGER.getFieldConverterType(), false, new QueryPath("field1", false)),
                new MapTableField("field2", INTEGER.getFieldConverterType(), false, new QueryPath("field2", false))
        );
        HazelcastTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(instance().getMap(mapName)), mapTableFields),
                1
        );
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(statement.getSql(), parameterTypes, table);

        assertPlan(
                optimizationResult.getPhysical(),
                plan(planRow(0, IndexScanMapPhysicalRel.class))
        );
    }

}
