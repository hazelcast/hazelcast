package com.hazelcast.jclouds;

import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class JCloudsPropertiesTest extends HazelcastTestSupport {

    @Test
    public void testConstructorIsPrivate()  {
        assertUtilityConstructor(JCloudsProperties.class);
    }

    @Test
    public void portValueValidator_should_not_throw_ValidationException_when_configured_in_range_values() throws Exception{
        try {
            JCloudsProperties.PortValueValidator valueValidator = new JCloudsProperties.PortValueValidator();
            valueValidator.validate(5701);
        } catch (ValidationException e) {
            fail("PortValueValidator should not throw ValidationException when configured in range values");
        }
    }

}
