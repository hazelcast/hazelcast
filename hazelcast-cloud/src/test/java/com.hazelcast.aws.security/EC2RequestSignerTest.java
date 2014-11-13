package com.hazelcast.aws.security;

import com.hazelcast.aws.impl.DescribeInstances;
import com.hazelcast.config.AwsConfig;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
    public void deriveSigningKeyTest() {
        // This is from http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setRegion(TEST_REGION);
        awsConfig.setHostHeader(TEST_HOST);
        awsConfig.setAccessKey(TEST_ACCESS_KEY);
        awsConfig.setSecretKey(TEST_SECRET_KEY);

        DescribeInstances di = new DescribeInstances(awsConfig);
        // Override the attributes map. We need to change values. Not pretty, but
        // no real alternative, and in this case : testing only
        try {
            Field field = null;
            Map<String, String> attributes = null;
            field = di.getClass().getDeclaredField("attributes");
            field.setAccessible(true);
            attributes = (Map<String, String>) field.get(di);
            attributes.put("X-Amz-Date", TEST_REQUEST_DATE);
            field.set(di, attributes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Override private method
        EC2RequestSigner rs = new EC2RequestSigner(awsConfig, TEST_REQUEST_DATE);
        byte[] derivedKey = null;
        try {
            Method method = null;
            method = rs.getClass().getDeclaredMethod("deriveSigningKey", null);
            method.setAccessible(true);
            derivedKey = (byte[]) method.invoke(rs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertEquals(TEST_DERIVED_EXPECTED, bytesToHex(derivedKey));
    }

    @Test
    public void testSigning() throws NoSuchFieldException, IllegalAccessException {
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setRegion(TEST_REGION);
        awsConfig.setHostHeader(TEST_HOST);
        awsConfig.setAccessKey(TEST_ACCESS_KEY);
        awsConfig.setSecretKey(TEST_SECRET_KEY);

        DescribeInstances di = new DescribeInstances(awsConfig);
        Field field = di.getClass().getDeclaredField("attributes");
        field.setAccessible(true);
        Map<String, String> attributes = (Map<String, String>) field.get(di);
        attributes.put("X-Amz-Date", TEST_REQUEST_DATE);
        field.set(di, attributes);

        EC2RequestSigner rs = new EC2RequestSigner(awsConfig, TEST_REQUEST_DATE);

        String signature = rs.sign(TEST_SERVICE, attributes);

        assertEquals(TEST_SIGNATURE_EXPECTED, signature);
    }


    private String bytesToHex(byte[] in) {
        final char[] hexArray = "0123456789abcdef".toCharArray();

        char[] hexChars = new char[in.length * 2];
        for ( int j = 0; j < in.length; j++ ) {
            int v = in[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
