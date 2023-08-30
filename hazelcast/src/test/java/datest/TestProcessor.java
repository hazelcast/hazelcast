package datest;

import com.hazelcast.map.EntryProcessor;

import java.util.Map;

public class TestProcessor implements EntryProcessor<String, TestPojo, TestPojo> {

    private static final long serialVersionUID = -7228575928294057876L;
    private final TestPojo newValue;

    TestProcessor(final TestPojo newValue) {
        this.newValue = newValue;
    }

    @Override
    public TestPojo process(final Map.Entry<String, TestPojo> entry) {
        System.out.println("Entry processor called to update new value: " + entry.getValue() + " with new value " + newValue);
        entry.getValue().setField1("ignoreIt");
        entry.setValue(newValue);
        return newValue;
    }
}
