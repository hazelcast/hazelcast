package com.hazelcast.aggregation;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static com.hazelcast.aggregation.TestSamples.createEntryWithValue;
import static com.hazelcast.aggregation.TestSamples.sampleBigDecimals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractAggregatorTest {

    @Test(timeout = TimeoutInMillis.MINUTE, expected = IllegalArgumentException.class)
    public void testBigDecimalAvg_whenNoAttributePathAndNoMapEntry_thenThrowException() {
        List<BigDecimal> values = sampleBigDecimals();

        Aggregator<BigDecimal, BigDecimal> aggregation = Aggregators.bigDecimalAvg();
        aggregation.accumulate(values.get(0));
    }

    @Test(timeout = TimeoutInMillis.MINUTE, expected = IllegalArgumentException.class)
    public void testBigDecimalAvg_whenWithAttributePathAndNoExtractable_thenThrowException() {
        List<BigDecimal> values = sampleBigDecimals();

        Aggregator<Map.Entry<BigDecimal, BigDecimal>, BigDecimal> aggregation = Aggregators.bigDecimalAvg("notFound");
        aggregation.accumulate(createEntryWithValue(values.get(0)));
    }
}
