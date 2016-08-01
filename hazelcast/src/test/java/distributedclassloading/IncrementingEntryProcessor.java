package distributedclassloading;

import com.hazelcast.map.AbstractEntryProcessor;

import java.util.Map;

/**
 * This test class is intentionally in its own package
 * as Hazelcast as a special rules for loading classes
 * from com.hazelcast.* package.
 *
 */
public class IncrementingEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

    @Override
    public Object process(Map.Entry<Integer, Integer> entry) {
        Integer origValue = entry.getValue();
        Integer newValue = origValue + 1;
        entry.setValue(newValue);

        return newValue;
    }
}
