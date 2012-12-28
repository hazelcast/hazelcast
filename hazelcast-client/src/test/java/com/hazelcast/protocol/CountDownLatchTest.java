/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.protocol;

import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class CountDownLatchTest extends ProtocolTest{


    @Test
    public void setCount() throws IOException {
        assertTrue(setCount(5).contains("true"));
    }

    private List<String> setCount(int count) throws IOException {
        OutputStream out = doOp("CDLSETCOUNT default "+count,null, socket);
        out.write("\r\n".getBytes());
        out.flush();
        return read(socket);
    }

    @Test
    public void getCount() throws IOException {
        setCount(5);
        OutputStream out = doOp("CDLGETCOUNT default",null, socket);
        out.write("\r\n".getBytes());
        out.flush();
        assertTrue(read(socket).contains("5"));
    }

}
