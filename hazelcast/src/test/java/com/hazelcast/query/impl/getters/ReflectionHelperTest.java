package com.hazelcast.query.impl.getters;

import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReflectionHelperTest {

    @Test
    public void test_getAttributeType() throws Exception {
        AttributeType attributeType = ReflectionHelper.getAttributeType(null, "test");

        assertNull(attributeType);
    }


    @Test
    public void extractValue_whenIntermediateFieldIsInterfaceAndDoesNotContainField_thenThrowIllegalArgumentException() throws Exception {
        OuterObject object = new OuterObject();
        try {
            ReflectionHelper.extractValue(object, "emptyInterface.doesNotExist");
            fail("Non-existing field has been ignored");
        } catch (QueryException e) {
            // createGetter() method is catching everything throwable and wraps it in QueryException
            // I don't think it's the right thing to do, but I don't want to change this behaviour.
            // Hence I have to use try/catch in this test instead of just declaring
            // IllegalArgumentException as expected exception.
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }



    private static class OuterObject {
        private EmptyInterface emptyInterface;
    }

    private interface EmptyInterface {

    }

}