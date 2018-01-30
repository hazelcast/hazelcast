/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AwsAddressTranslatorTest {

    private final Map<String, String> lookup = new HashMap<String, String>();

    private Address privateAddress;
    private Address publicAddress;

    private AWSClient awsClient;
    private ClientAwsConfig config;
    private LoggingService loggingService;
    private AwsAddressTranslator translator;

    @Before
    public void setUp() throws Exception {
        privateAddress = new Address("127.0.0.1", 5701);
        publicAddress = new Address("192.168.0.1", 5701);

        awsClient = mock(AWSClient.class);
        when(awsClient.getAddresses()).thenReturn(lookup);

        config = new ClientAwsConfig();
        config.setIamRole("anyRole");

        ILogger logger = Logger.getLogger(AwsAddressTranslatorTest.class);

        loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(eq(AwsAddressTranslator.class))).thenReturn(logger);

        translator = new AwsAddressTranslator(config, loggingService);
    }

    @Test
    public void testTranslate_whenInsideAws_thenReturnSameAddress() {
        config.setInsideAws(true);
        AwsAddressTranslator translator = new AwsAddressTranslator(config, loggingService);

        Address actual = translator.translate(privateAddress);

        assertEquals(privateAddress, actual);
    }

    @Test
    public void testTranslate_whenAddressIsNull_thenReturnNull() {
        Address actual = translator.translate(null);

        assertNull(actual);
    }

    @Test
    public void testTranslate_whenAddressIsMapped_thenReturnPublicAddress() {
        translator.getLookupTable().put(privateAddress.getHost(), publicAddress.getHost());

        Address actual = translator.translate(privateAddress);

        assertEquals(publicAddress.getHost(), actual.getHost());
        assertEquals(privateAddress.getPort(), actual.getPort());
    }

    @Test
    public void testTranslate_whenPublicAddress_thenReturnCachedValue() {
        translator.getLookupTable().put(privateAddress.getHost(), publicAddress.getHost());

        Address actual = translator.translate(publicAddress);

        assertEquals(publicAddress.getHost(), actual.getHost());
        assertEquals(privateAddress.getPort(), actual.getPort());
    }

    @Test
    public void testTranslate_whenNotMapped_thenRefresh() {
        translator = new AwsAddressTranslator(awsClient, config, loggingService);

        lookup.put(privateAddress.getHost(), publicAddress.getHost());

        Address actual = translator.translate(privateAddress);

        assertEquals(publicAddress.getHost(), actual.getHost());
        assertEquals(privateAddress.getPort(), actual.getPort());
    }

    @Test
    public void testTranslate_whenNotFound_thenReturnNull() {
        translator = new AwsAddressTranslator(awsClient, config, loggingService);

        Address actual = translator.translate(privateAddress);

        assertNull(actual);
    }

    @Test
    public void testTranslate_whenPublicAddressIsInvalid_thenReturnNull() {
        translator.getLookupTable().put(privateAddress.getHost(), "invalidHost");

        Address actual = translator.translate(privateAddress);

        assertNull(actual);
    }

    @Test
    public void testRefresh_whenException_thenLogWarning() throws Exception {
        when(awsClient.getAddresses()).thenThrow(new RuntimeException("expected exception!"));

        translator = new AwsAddressTranslator(awsClient, config, loggingService);

        translator.refresh();
    }
}
