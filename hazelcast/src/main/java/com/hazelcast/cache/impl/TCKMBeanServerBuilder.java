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

package com.hazelcast.cache.impl;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

/**
 * An MBeanServer builder required by TCK tests. Has no function in the implementation itself.
 */
//TODO should we move this into tests?
public class TCKMBeanServerBuilder
        extends MBeanServerBuilder {

    public TCKMBeanServerBuilder() {
    }

    @Override
    public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
        return super.newMBeanServer(defaultDomain, outer, new RIMBeanServerDelegate(delegate));
    }

    public static class RIMBeanServerDelegate
            extends MBeanServerDelegate {

        private MBeanServerDelegate delegate;

        public RIMBeanServerDelegate(MBeanServerDelegate delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getSpecificationName() {
            return delegate.getSpecificationName();
        }

        @Override
        public String getSpecificationVersion() {
            return delegate.getSpecificationVersion();
        }

        @Override
        public String getSpecificationVendor() {
            return delegate.getSpecificationVendor();
        }

        @Override
        public String getImplementationName() {
            return delegate.getImplementationName();
        }

        @Override
        public String getImplementationVersion() {
            return delegate.getImplementationVersion();
        }

        @Override
        public String getImplementationVendor() {
            return delegate.getImplementationVendor();
        }

        @Override
        public MBeanNotificationInfo[] getNotificationInfo() {
            return delegate.getNotificationInfo();
        }

        @Override
        public synchronized void addNotificationListener(NotificationListener listener, NotificationFilter filter,
                                                         Object handback)
                throws IllegalArgumentException {
            delegate.addNotificationListener(listener, filter, handback);
        }

        @Override
        public synchronized void removeNotificationListener(NotificationListener listener, NotificationFilter filter,
                                                            Object handback)
                throws ListenerNotFoundException {
            delegate.removeNotificationListener(listener, filter, handback);
        }

        @Override
        public synchronized void removeNotificationListener(NotificationListener listener)
                throws ListenerNotFoundException {
            delegate.removeNotificationListener(listener);
        }

        @Override
        public void sendNotification(Notification notification) {
            delegate.sendNotification(notification);
        }

        @Override
        public synchronized String getMBeanServerId() {
            return System.getProperty("org.jsr107.tck.management.agentId");
        }
    }

}
