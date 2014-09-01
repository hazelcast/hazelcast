package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicate.AbstractPredicate;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.SqlPredicate#createPredicate}
 * You can generate predicates using {@link com.hazelcast.query.impl.predicate.SqlPredicate#createPredicate} static method.
 */
@Deprecated
public class SqlPredicate extends AbstractPredicate implements IndexAwarePredicate {

    private static final long serialVersionUID = 1;
    private transient Predicate predicate;
    private String sql;

    public SqlPredicate(String sql) {
        this.sql = sql;
        predicate = createPredicate(sql);
    }

    public SqlPredicate() {
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return predicate.apply(mapEntry);
    }

    @Override
    public boolean isSubSet(Predicate predicate) {
        return this.predicate.isSubSet(predicate);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return ((IndexAwarePredicate) predicate).filter(queryContext);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(sql);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sql = in.readUTF();
        predicate = createPredicate(sql);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
    }

    private Predicate createPredicate(String sql) {
       return com.hazelcast.query.impl.predicate.SqlPredicate.createPredicate(sql);
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        predicate = createPredicate(sql);
    }

    @Override
    public String toString() {
        return predicate.toString();
    }
}
