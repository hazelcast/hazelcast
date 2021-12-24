package com.hazelcast.jet.sql.impl.expression.json;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * We serialize some Calcite enums by using their ordinal. There could
 * be an incompatible change in Calcite that would break it. This test
 * checks for such a change.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CalciteEnumStabilityTest {

    @Test
    public void test_SqlJsonQueryWrapperBehavior() throws Exception {
        compareActualValues(SqlJsonQueryWrapperBehavior.class,
                "WITHOUT_ARRAY",
                "WITH_CONDITIONAL_ARRAY",
                "WITH_UNCONDITIONAL_ARRAY");
    }

    @Test
    public void test_SqlJsonQueryEmptyOrErrorBehavior() throws Exception {
        compareActualValues(SqlJsonQueryEmptyOrErrorBehavior.class,
                "ERROR", "NULL", "EMPTY_ARRAY", "EMPTY_OBJECT");
    }

    private void compareActualValues(Class<? extends Enum<?>> enumClass, String... expected) throws Exception {
        Enum[] actualValues = (Enum[]) enumClass.getDeclaredMethod("values").invoke(null);
        List<String> actualValuesStrings = Arrays.stream(actualValues).map(Enum::name).collect(Collectors.toList());
        assertEquals(asList(expected), actualValuesStrings);
    }
}
