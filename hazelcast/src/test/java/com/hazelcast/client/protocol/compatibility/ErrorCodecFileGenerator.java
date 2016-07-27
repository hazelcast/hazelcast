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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * This class is used for generating the binary file to be committed at the beginning of
 * introducing a new serialization service. Change version field and run this class once.
 * Then move the created files to resources directory.
 * <p/>
 * mv *binary hazelcast/src/test/resources/
 */
public class ErrorCodecFileGenerator {

    public static void main(String[] args) throws IOException {
        Map<String, Throwable[]> allThrowables = ReferenceObjects.throwables;
        ClientExceptionFactory clientExceptionFactory = new ClientExceptionFactory(true);
        for (String version : allThrowables.keySet()) {
            String fileName = createFileName(version);
            OutputStream out = new FileOutputStream(fileName);
            DataOutputStream outputStream = new DataOutputStream(out);
            Throwable[] throwables = allThrowables.get(version);
            for (Throwable throwable : throwables) {
                outputStream.writeUTF(createObjectKey(throwable, version));
                ClientMessage clientMessage = clientExceptionFactory.createExceptionMessage(throwable);
                outputStream.writeInt(clientMessage.getFrameLength());
                outputStream.write(clientMessage.buffer().byteArray(), 0, clientMessage.getFrameLength());
            }
            outputStream.close();
        }
    }

    private static String createObjectKey(Object object, String version) {
        return version + "-" + object.getClass().getSimpleName();
    }

    private static String createFileName(String version) {
        return version + ".protocol.errorCodec.compatibility.binary";
    }
}
