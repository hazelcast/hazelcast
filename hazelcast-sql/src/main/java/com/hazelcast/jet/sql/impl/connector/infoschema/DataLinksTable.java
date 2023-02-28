package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.datalink.DataLink;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

public class DataLinksTable extends InfoSchemaTable {
    private static final String NAME = "datalinks";

    private static final List<TableField> FIELDS = asList(
            new TableField("table_catalog", QueryDataType.VARCHAR, false),
            new TableField("table_schema", QueryDataType.VARCHAR, false),
            new TableField("datalink_name", QueryDataType.VARCHAR, false),
            new TableField("datalink_type", QueryDataType.VARCHAR, false),
            new TableField("datalink_options", QueryDataType.VARCHAR, false)
    );

    private final String dataLinkSchema;
    private final Collection<DataLink> dataLinks;

    public DataLinksTable(String catalog,
                          String schemaName,
                          String dataLinkSchema,
                          Collection<DataLink> dataLinks) {
        super(FIELDS, catalog, schemaName, NAME, new ConstantTableStatistics(0));
        this.dataLinkSchema = dataLinkSchema;
        this.dataLinks = dataLinks;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(dataLinks.size());
        for (DataLink dl : dataLinks) {
            Object[] row = new Object[]{
                    catalog(),
                    dataLinkSchema,
                    dl.getName(),
                    dl.getType(),
                    dl.getOptions().toString()
            };
            rows.add(row);
        }
        return rows;
    }
}
