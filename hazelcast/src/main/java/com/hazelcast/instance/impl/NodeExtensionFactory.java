/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ServiceLoader;

import java.util.Iterator;
import java.util.List;

public final class NodeExtensionFactory {

    private static final String NODE_EXTENSION_FACTORY_ID = "com.hazelcast.instance.impl.NodeExtension";

    private NodeExtensionFactory() {
    }

    /**
     * Uses the Hazelcast ServiceLoader to discover all registered {@link
     * NodeExtension} classes and identify the one to instantiate and use as
     * the provided {@code node}'s extension. It chooses the class based on
     * the provided priority list of class names, these are the rules:
     * <ol><li>
     *     A class's priority is its zero-based index in the list.
     * </li><li>
     *     Lower number means higher priority.
     * </li><li>
     *     A class that doesn't appear in the list doesn't qualify for selection.
     * </li></ol>
     * <p>
     * The dynamic selection of the node extension allows Hazelcast
     * Enterprise's JAR to be swapped in for the core Hazelcast JAR with no
     * changes to user's code or configuration. Hazelcast core code can call
     * this method with a priority list naming both the default and the
     * enterprise node extension and it will automatically prefer the
     * Enterprise one when present.
     * <p>
     * The explicit priority list is necessary because a Hazelcast Jet JAR
     * contains both the default node extension and the Jet node extension,
     * but the one to choose depends on whether the user is requesting to
     * start an IMDG or a Jet instance.
     *
     * @param node Hazelcast node whose extension this method must provide
     * @param extensionPriorityList priority list of fully qualified extension class names
     * @return the selected node extension
     */
    public static NodeExtension create(Node node, List<String> extensionPriorityList) {
        try {
            ClassLoader classLoader = node.getConfigClassLoader();
            Class<NodeExtension> chosenExtension = null;
            int chosenPriority = Integer.MAX_VALUE;
            for (Iterator<Class<NodeExtension>> iter =
                 ServiceLoader.classIterator(NodeExtension.class, NODE_EXTENSION_FACTORY_ID, classLoader);
                 iter.hasNext();
            ) {
                Class<NodeExtension> currExt = iter.next();
                warnIfDuplicate(currExt);
                int currPriority = extensionPriorityList.indexOf(currExt.getName());
                if (currPriority == -1) {
                    continue;
                }
                if (currPriority < chosenPriority) {
                    chosenPriority = currPriority;
                    chosenExtension = currExt;
                }
            }
            if (chosenExtension == null) {
                throw new HazelcastException("ServiceLoader didn't find any services registered under "
                        + NODE_EXTENSION_FACTORY_ID);
            }
            return chosenExtension.getConstructor(Node.class).newInstance(node);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
   }

    private static void warnIfDuplicate(Class<NodeExtension> klass) {
        if (!klass.equals(DefaultNodeExtension.class) && klass.getName().equals(DefaultNodeExtension.class.getName())) {
            Logger.getLogger(NodeExtensionFactory.class).warning(
                    "DefaultNodeExtension class has been loaded by two different class-loaders.\n"
                            + "Classloader 1: " + NodeExtensionFactory.class.getClassLoader() + '\n'
                            + "Classloader 2: " + klass.getClassLoader() + '\n'
                            + "Are you running Hazelcast Jet in an OSGi environment?"
                            + " If so, set the bundle class-loader in the Config using the setClassloader() method");
        }
    }
}
