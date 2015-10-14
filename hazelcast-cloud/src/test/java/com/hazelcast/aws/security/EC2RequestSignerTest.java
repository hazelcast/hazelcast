/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.aws.security;

import com.hazelcast.aws.impl.Constants;
import com.hazelcast.aws.impl.DescribeInstances;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by igmar on 03/11/14.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EC2RequestSignerTest {
    private final static String TEST_REGION = "eu-central-1";
    private final static String TEST_HOST = "ec2.eu-central-1.amazonaws.com";
    private final static String TEST_SERVICE = "ec2";
    private final static String TEST_ACCESS_KEY = "AKIDEXAMPLE";
    private final static String TEST_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private final static String TEST_REQUEST_DATE = "20141106T111126Z";
    private final static String TEST_DERIVED_EXPECTED = "7038265e40236063ebcd2e201908ad6e9f64e533439bfa7a5faa07ba419329bc";
    private final static String TEST_SIGNATURE_EXPECTED = "c9347599958aab0ea079c296b8fe3355553bac767c5957dff7e7a1fce72ce132";


    @Test
    public void test_Constasts() {
        Constants constants = new Constants();
        assertNotNull(constants.DATE_FORMAT);
        assertNotNull(constants.DOC_VERSION);
        assertNotNull(constants.SIGNATURE_METHOD_V4);
        assertNotNull(constants.GET);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenConfigIsNull() {
        new EC2RequestSigner(null, "", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenTimeStampIsNull() {
        new EC2RequestSigner(new AwsConfig(), null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenTimeSignServiceIsNull() {
        EC2RequestSigner signer = new EC2RequestSigner(new AwsConfig(), "", "");

        signer.sign(null, new HashMap<String, String>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenTimeSignAttributeIsNull() {
        EC2RequestSigner signer = new EC2RequestSigner(new AwsConfig(), "", "");

        signer.sign("", null);
    }

    @Test
    public void deriveSigningKeyTest() throws Exception {
        // This is from http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setRegion(TEST_REGION).
                setHostHeader(TEST_HOST).
                setAccessKey(TEST_ACCESS_KEY).
                setSecretKey(TEST_SECRET_KEY);

        DescribeInstances di = new DescribeInstances(awsConfig, TEST_HOST);
        // Override the attributes map. We need to change values. Not pretty, but
        // no real alternative, and in this case : testing only

        Field field = di.getClass().getDeclaredField("attributes");
        field.setAccessible(true);
        Map<String, String> attributes = (Map<String, String>) field.get(di);
        attributes.put("X-Amz-Date", TEST_REQUEST_DATE);
        field.set(di, attributes);

        // Override private method
        EC2RequestSigner rs = new EC2RequestSigner(awsConfig, TEST_REQUEST_DATE, TEST_HOST);
        field = rs.getClass().getDeclaredField("service");
        field.setAccessible(true);
        field.set(rs, "ec2");

        Method method = rs.getClass().getDeclaredMethod("deriveSigningKey", null);
        method.setAccessible(true);
        byte[] derivedKey = (byte[]) method.invoke(rs);

        assertEquals(TEST_DERIVED_EXPECTED, bytesToHex(derivedKey));
    }

    @Test
    public void testSigning() throws NoSuchFieldException, IllegalAccessException {
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setRegion(TEST_REGION).
                setHostHeader(TEST_HOST).
                setAccessKey(TEST_ACCESS_KEY).
                setSecretKey(TEST_SECRET_KEY);

        DescribeInstances di = new DescribeInstances(awsConfig, TEST_HOST);

        Field attributesField = di.getClass().getDeclaredField("attributes");
        attributesField.setAccessible(true);
        Map<String, String> attributes = (Map<String, String>) attributesField.get(di);
        attributes.put("X-Amz-Date", TEST_REQUEST_DATE);

        EC2RequestSigner rs = new EC2RequestSigner(awsConfig, TEST_REQUEST_DATE, TEST_HOST);
        attributes.put("X-Amz-Credential", rs.createFormattedCredential());
        String signature = rs.sign(TEST_SERVICE, attributes);

        assertEquals(TEST_SIGNATURE_EXPECTED, signature);
    }


    private String bytesToHex(byte[] in) {
        final char[] hexArray = "0123456789abcdef".toCharArray();

        char[] hexChars = new char[in.length * 2];
        for (int j = 0; j < in.length; j++) {
            int v = in[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
