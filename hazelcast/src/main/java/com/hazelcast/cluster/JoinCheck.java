/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.impl.Node;
import com.hazelcast.impl.spi.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @mdogan 8/2/12
 */
public class JoinCheck extends AbstractOperation implements NonBlockingOperation, NonMemberOperation {

    private JoinInfo joinInfo;

    public JoinCheck() {
    }

    public JoinCheck(final JoinInfo joinInfo) {
        this.joinInfo = joinInfo;
    }

    public void run() {
        System.out.println("RUNNING CHECK !!!!! -> " + joinInfo);
        OperationContext context = getOperationContext();
        Node node = context.getNodeService().getNode();
        boolean ok = false;
        if (joinInfo != null && node.joined() && node.isActive()) {
            try {
                ok = node.validateJoinRequest(joinInfo);
            } catch (Exception ignored) {
            }
        }
        if (ok) {
            context.getResponseHandler().sendResponse(new Response(node.createJoinInfo()));
        } else {
            context.getResponseHandler().sendResponse(null);
        }
    }

    @Override
    public void readData(final DataInput in) throws IOException {
        super.readData(in);
        System.out.println("READING =============");
        joinInfo = new JoinInfo();
        joinInfo.readData(in);
    }

    @Override
    public void writeData(final DataOutput out) throws IOException {
        super.writeData(out);
        System.out.println("WRITING =============");
        joinInfo.writeData(out);
    }
}

