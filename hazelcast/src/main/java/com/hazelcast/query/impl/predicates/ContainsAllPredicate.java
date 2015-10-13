package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.collection.ArrayUtils;
import com.hazelcast.util.collection.InflatableSet;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.Preconditions.isNotNull;

public final class ContainsAllPredicate extends AbstractPredicate implements Predicate {

    private Set<Comparable> values;

    ContainsAllPredicate(String attribute, Set<Comparable> values) {
        super(attribute);
        isNotNull(values, "Values cannot be null");
        this.values = values;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        for (Comparable value : values) {
            Object o = readAttributeValue(mapEntry);
            if (o instanceof Object[]) {
                Object[] attributes = (Object[]) o;
                boolean valueFound = ArrayUtils.contains(attributes, value);
                if (!valueFound) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        /**
         * The left-operand of the ContainsAll predicate is a collection.
         * In order to use indexing here we would need to have the values of the collection indexed.
         * At first, there was an idea to "flatten" the collection and store it's values in an index.
         *
         * It generates problems, however, e.g. in the EqualPredicate and a collection as the left-operand.
         * Let's consider: person.body.limbs (where limbs is a collection of strings like "hand", "leg", etc.)
         * If we add an index addIndex("person.body.limbs") it would store "hand" and "leg" in the index.
         * It would work fine in the ContainsPredicate -> since if we did 'person.body.limbs contains "hand"'
         * we could leverage the index.
         * If we however used the EqualPredicate and use the filter() method to filter out all matching values for
         * for the expression 'person.body.limbs == "hand"' it would return a wrong result, since it would
         * get the value from the index that doesn't have the piece of information that the "hadn" was added there
         * due to "flattening" of the collection.
         * Of course, the 'person.body.limbs == "hand"' is not a valid expression since the left-side is a collection
         * the right-side is a single-value expression, but this is unknown to the EqualsPredicate when the expression
         * is parsed. It is known only if we touch the real data - and in order to spot it's not enough to fetch the
         * value from the index. We then need to take the QueryEntry, execute the readAttribute on the first one
         * and see if the extracted value is a collection or not.
         * It would all be too "hacky".
         *
         * It is possible to use some "trickery" to use indexing here in the second shot.
         * While evaluating the expression 'person.body.limbs containsAll ["hand", "leg"]'
         * we could check if there's an index for 'person.body.limbs[*]', if so we could use it to find persons
         * whose 'person.body.limbs[*] is "hand"', then 'person.body.limbs[*] is "leg"' - then take the smaller
         * result and see if the other limb is also in the collection.
         * Downside is that if there's a custom extractor registered to index person.body.limbs[*] we won't be able
         * to leverage it.
         *
         * Current state of the art: ContainsAll does not use indexes. If the underlying collection is a Set with a
         * O(1) lookup time the complexity of apply() method will be as if an index has been used.
         */
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(values.size());
        for (Comparable c : values) {
            out.writeObject(c);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        int size = in.readInt();
        InflatableSet.Builder<Comparable> builder = InflatableSet.newBuilder(size);
        for (int i = 0; i < size; i++) {
            Comparable o = in.readObject();
            builder.add(o);
        }
        values = builder.build();
    }
}
