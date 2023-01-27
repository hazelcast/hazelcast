package com.hazelcast.jet.mongodb.sql;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashSet;
import java.util.List;

class DocumentQueryTarget implements QueryTarget {

    private final List<String> fieldList;
    private Object[] entry;

    DocumentQueryTarget(List<String> fieldList) {
        this.fieldList = fieldList;
        assert new HashSet<>(fieldList).size() == fieldList.size() : "duplicates in the fieldList";
    }

    @Override
    public void setTarget(Object target, Data targetData) {
        assert targetData == null;
        Object[] values = ((JetSqlRow) target).getValues();
        entry = values;
        assert entry.length == fieldList.size();
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return path == null ? createExtractor() : createFieldExtractor(path, type);
    }

    private QueryExtractor createExtractor() {
        return () -> entry;
    }

    private QueryExtractor createFieldExtractor(String path, QueryDataType type) {
        int fieldIndex = fieldList.indexOf(path);
        return () -> type.convert(entry[fieldIndex]);
    }
}
