package com.hazelcast.jclouds;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.jclouds.aws.domain.SessionCredentials;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IAMRoleCredentialSupplierBuilderTest extends HazelcastTestSupport {


    @Test(expected = IllegalArgumentException.class)
    public void whenIamRoleIsNull() {
        new IAMRoleCredentialSupplierBuilder().withRoleName(null);
    }

    @Test
    public void whenIamRoleIsNotNull() {
        IAMRoleCredentialSupplierBuilder builder =
                new IAMRoleCredentialSupplierBuilder().withRoleName("role");
        assertNull(builder.getCredentials());
        assertEquals(builder.getRoleName(), "role");
    }

    @Test
    public void testParseIamRole() throws IOException {
        String s = "{\n" +
                "  \"Code\" : \"Success\",\n" +
                "  \"LastUpdated\" : \"2015-09-06T21:17:26Z\",\n" +
                "  \"Type\" : \"AWS-HMAC\",\n" +
                "  \"AccessKeyId\" : \"ASIAIEXAMPLEOXYDA\",\n" +
                "  \"SecretAccessKey\" : \"hOCVge3EXAMPLExSJ+B\",\n" +
                "  \"Token\" : \"AQoDYXdzEE4EXAMPLE2UGAFshkTsyw7gojLdiEXAMPLE+1SfSRTfLR\",\n" +
                "  \"Expiration\" : \"2015-09-07T03:19:56Z\"\n}";
        StringReader sr = new StringReader(s);
        BufferedReader br = new BufferedReader(sr);
        IAMRoleCredentialSupplierBuilder iamRoleCredentialSupplierBuilder = new IAMRoleCredentialSupplierBuilder();
        Map map = iamRoleCredentialSupplierBuilder.parseIamRole(br);
        assertEquals("Success", map.get("Code"));
        assertEquals("2015-09-06T21:17:26Z", map.get("LastUpdated"));
        assertEquals("AWS-HMAC", map.get("Type"));
        assertEquals("ASIAIEXAMPLEOXYDA", map.get("AccessKeyId"));
        assertEquals("hOCVge3EXAMPLExSJ+B", map.get("SecretAccessKey"));
        assertEquals("AQoDYXdzEE4EXAMPLE2UGAFshkTsyw7gojLdiEXAMPLE+1SfSRTfLR", map.get("Token"));
    }

    @Test
    public void testBuild() throws IOException {
        IAMRoleCredentialSupplierBuilder iamRoleCredentialSupplierBuilder = mock(IAMRoleCredentialSupplierBuilder.class);
        Map map = new HashMap();
        map.put("AccessKeyId", "ExampleAccessKeyId");
        map.put("SecretAccessKey", "ExampleSecretAccessKey");
        map.put("Token", "ExampleToken");
        when(iamRoleCredentialSupplierBuilder.getKeysFromIamRole()).thenReturn(map);
        when(iamRoleCredentialSupplierBuilder.build()).thenCallRealMethod();
        iamRoleCredentialSupplierBuilder.withRoleName("example");
        SessionCredentials sessionCredentials = iamRoleCredentialSupplierBuilder.build();
        assertEquals("ExampleAccessKeyId", sessionCredentials.getAccessKeyId());
        assertEquals("ExampleSecretAccessKey", sessionCredentials.getSecretAccessKey());
        assertEquals("ExampleToken", sessionCredentials.getSessionToken());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void parseIamRole_withInvalidAwsConfiguration() {
        new IAMRoleCredentialSupplierBuilder().getKeysFromIamRole();
    }

}
