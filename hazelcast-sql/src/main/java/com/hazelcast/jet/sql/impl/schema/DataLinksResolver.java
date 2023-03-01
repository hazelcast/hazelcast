package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.infoschema.DataLinksTable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.datalink.DataLink;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_INFORMATION_SCHEMA;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class DataLinksResolver implements TableResolver {
    // We contain separate schema, so separate resolver was implemented.
    private static final List<List<String>> SEARCH_PATHS = singletonList(
            asList(CATALOG, SCHEMA_NAME_PUBLIC)
    );

    private static final List<Function<List<DataLink>, Table>> ADDITIONAL_TABLE_PRODUCERS = singletonList(
            dl -> new DataLinksTable(CATALOG, SCHEMA_NAME_INFORMATION_SCHEMA, SCHEMA_NAME_PUBLIC, dl)
    );

    private final DataLinkStorage dataLinkStorage;

    public DataLinksResolver(DataLinkStorage dataLinkStorage) {
        this.dataLinkStorage = dataLinkStorage;
    }

    public void createDataLink(DataLink dl, boolean replace, boolean ifNotExists) {
        if (ifNotExists) {
            dataLinkStorage.putIfAbsent(dl.getName(), dl);
        } else if (replace) {
            dataLinkStorage.put(dl.getName(), dl);
        } else if (!dataLinkStorage.putIfAbsent(dl.getName(), dl)) {
            throw QueryException.error("Data link already exists: " + dl.getName());
        }
    }

    public void removeDataLink(String name, boolean ifExists) {
        if (dataLinkStorage.removeDataLink(name) == null && !ifExists) {
            throw QueryException.error("Data link does not exist: " + name);
        }
    }

    @Nonnull
    public Collection<String> getDataLinkNames() {
        return dataLinkStorage.dataLinkNames();
    }

    @Nonnull
    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return SEARCH_PATHS;
    }

    @Nonnull
    @Override
    public List<Table> getTables() {
        List<Table> tables = new ArrayList<>();
        ADDITIONAL_TABLE_PRODUCERS.forEach(
                producer -> tables.add(producer.apply(dataLinkStorage.dataLinks())));
        return tables;
    }

    @Override
    public void registerListener(TableListener listener) {
    }
}
