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

package com.hazelcast.jca;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JcaBase {
    protected static final Logger logger = Logger.getLogger(JcaBase.class.getName());

    public void log(final Object caller, final Object msg) {
        logger.log(Level.FINEST, caller + " : " + msg);
    }
}
