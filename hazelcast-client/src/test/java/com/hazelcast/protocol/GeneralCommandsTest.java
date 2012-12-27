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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class GeneralCommandsTest extends ProtocolTest{

    @Test
    public void destroy() throws IOException {
        OutputStream out = doOp("MGET default #1", "1");
        out.write("a".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        read(socket);


        doOp("DESTROY 0 map default ", null, socket);
        assertTrue(read(socket).contains("OK"));
    }

    @Test
    public void instances() throws IOException {
        OutputStream out = doOp("MGET default #1", "1");
        out.write("a".getBytes());
        out.write("\r\n".getBytes());
        out.flush();
        read(socket);

        doOp("INSTANCES 0", null, socket);
        List<String> list = read(socket);
        assertTrue(list.contains("MAP"));
        assertTrue(list.contains("default"));
    }

    @Test
    public void members() throws IOException {
        OutputStream out = doOp("MEMBERS", null);
        out.flush();
        List<String> list = read(socket);
//        for(Member member: Hazelcast.getCluster().getMembers()){
//            assertTrue(list.contains(member.getInetSocketAddress().toString()));
//        }
    }
}

