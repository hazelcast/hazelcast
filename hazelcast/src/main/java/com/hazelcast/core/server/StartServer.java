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

package com.hazelcast.core.server;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

/**
 * Starts a Hazelcast Member.
 *
 * @deprecated Use {@link HazelcastMemberStarter} instead.
 */
@Deprecated
public final class StartServer {

    private StartServer() {
    }

    /**
     * Starts a Hazelcast Member.
     *
     * @deprecated Use {@link HazelcastMemberStarter} instead.
     */
    @Deprecated
    public static void main(String[] args)
            throws FileNotFoundException, UnsupportedEncodingException {
        HazelcastMemberStarter.main(args);
    }
}
