package com.hazelcast.query.impl.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.ConnectorPredicate;
import com.hazelcast.query.Predicate;

import java.io.IOException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 29/08/14 15:02.
 */
public abstract class AbstractConnectorPredicate implements ConnectorPredicate, DataSerializable {
    protected Predicate[] predicates;

    protected AbstractConnectorPredicate() {
    }


    // ConnectorPredicate instances must have more than one predicate.
    public AbstractConnectorPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        if (predicates.length > 0) {
            this.predicates = new Predicate[predicates.length + 2];
            this.predicates[0] = predicate1;
            this.predicates[1] = predicate2;
            System.arraycopy(predicates, 0, this.predicates, 2, predicates.length);
        } else {
            this.predicates = new Predicate[]{predicate1, predicate2};
        }
    }

    @Override
    public Predicate[] getPredicates() {
        return predicates;
    }

    @Override
    public int getPredicateCount() {
        int i = 0;
        for (Predicate predicate : predicates) {
            if (predicate instanceof ConnectorPredicate) {
                i += ((ConnectorPredicate) predicate).getPredicateCount();
            } else {
                i++;
            }
        }
        return i;
    }

    public String toStringInternal(String parameterName) {
        final StringBuilder sb = new StringBuilder();
        sb.append("(");
        int size = predicates.length;
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(" " + parameterName + " ");
            }
            sb.append(predicates[i]);
        }
        sb.append(")");
        return sb.toString();
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(predicates.length);
        for (Predicate predicate : predicates) {
            out.writeObject(predicate);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        predicates = new Predicate[size];
        for (int i = 0; i < size; i++) {
            predicates[i] = in.readObject();
        }
    }


    @Override
    public boolean isSubSet(Predicate predicate) {
        for (Predicate p : predicates) {
            if (p.isSubSet(predicate)) {
                return true;
            }
        }
        return false;
    }
}
