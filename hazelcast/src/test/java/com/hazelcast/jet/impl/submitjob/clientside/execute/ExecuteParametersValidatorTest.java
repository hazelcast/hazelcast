package com.hazelcast.jet.impl.submitjob.clientside.execute;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SubmitJobParameters;
import org.junit.Test;

import java.nio.file.Paths;

import static com.hazelcast.jet.core.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExecuteParametersValidatorTest {
    @Test
    public void nullJarPath() {
        ExecuteParametersValidator validator = new ExecuteParametersValidator();

        SubmitJobParameters parameterObject = new SubmitJobParameters();
        assertThatThrownBy(() -> validator.validate(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }


    @Test
    public void invalidFileExtension() {
        ExecuteParametersValidator validator = new ExecuteParametersValidator();
        SubmitJobParameters parameterObject = new SubmitJobParameters();
        parameterObject.setJarPath(Paths.get("/mnt/foo"));

        assertThatThrownBy(() -> validator.validate(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File name extension should be .jar");

    }

    @Test
    public void nullJobParameters() {
        ExecuteParametersValidator validator = new ExecuteParametersValidator();

        SubmitJobParameters parameterObject = new SubmitJobParameters();
        parameterObject.setJarPath(getJarPath());
        parameterObject.setJobParameters(null);
        assertThatThrownBy(() -> validator.validate(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }
}