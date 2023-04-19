package com.hazelcast.jet.impl.submitjob.memberside.validator;

import com.hazelcast.jet.JetException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JarOnClientValidatorTest {

    @Test
    public void testValidateTempDirectoryPath() {
        assertThatThrownBy(() -> JarOnClientValidator.validateTempDirectoryPath("foo"))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("The temporary file path does not exist: foo");
    }

    @Test
    public void testValidateJobParameters() {
        assertThatThrownBy(() -> JarOnClientValidator.validateJobParameters(null))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }
}
