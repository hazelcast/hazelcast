/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol.compatibility;

import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.nio.serialization.compatibility.BinaryCompatibilityTest;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static com.hazelcast.client.protocol.compatibility.ReferenceObjects.isEqual;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ErrorCodecCompatibilityTest {

    private static Map<String, byte[]> dataMap = new HashMap<String, byte[]>();
    private static ClientExceptionFactory clientExceptionFactory = new ClientExceptionFactory(true);
    //OPTIONS
    private static Map<String, Throwable[]> throwables = ReferenceObjects.throwables;
    private static String[] versions = {"1.0", "1.1"};

    @Parameterized.Parameter(0)
    public Throwable throwable;
    @Parameterized.Parameter(1)
    public String version;

    @BeforeClass
    public static void init() throws IOException {
        for (String version : versions) {
            String fileName = createFileName(version);
            InputStream input = BinaryCompatibilityTest.class.getResourceAsStream("/" + fileName);
            DataInputStream inputStream = new DataInputStream(input);
            while (input.available() != 0) {
                String objectKey = inputStream.readUTF();
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.read(bytes);
                dataMap.put(objectKey, bytes);
            }
            inputStream.close();
        }
    }

    private String createObjectKey() {
        return version + "-" + throwable.getClass().getSimpleName();
    }

    private static String createFileName(String version) {
        return version + ".protocol.errorCodec.compatibility.binary";
    }

    @Parameterized.Parameters(name = "throwable:{0}, version:{1}")
    public static Iterable<Object[]> parameters() {
        LinkedList<Object[]> parameters = new LinkedList<Object[]>();
        for (String version : versions) {
            for (Throwable throwable : throwables.get(version)) {
                parameters.add(new Object[]{throwable, version});
            }
        }
        return parameters;
    }

    @Test
    public void readAndVerifyBinaries() throws IOException {
        String key = createObjectKey();
        byte[] bytes = dataMap.get(key);
        Throwable exception = clientExceptionFactory.createException(ClientMessage.createForDecode(new SafeBuffer(bytes), 0));
        Assert.assertTrue(isEqual(throwable.getClass().getName(), exception.getClass().getName()));
    }
}
