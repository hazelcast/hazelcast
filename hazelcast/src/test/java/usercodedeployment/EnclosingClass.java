package usercodedeployment;

import com.hazelcast.map.AbstractEntryProcessor;

import java.util.Map;

public class EnclosingClass {

    public static class StaticNestedIncrementingEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            Integer origValue = entry.getValue();
            Integer newValue = origValue + 1;
            entry.setValue(newValue);

            return newValue;
        }
    }

    public static class StaticNestedDecrementingEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            Integer origValue = entry.getValue();
            Integer newValue = origValue - 1;
            entry.setValue(newValue);

            return newValue;
        }
    }
}
