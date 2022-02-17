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

package com.hazelcast.osgi;

import com.hazelcast.osgi.impl.Activator;
import com.hazelcast.internal.util.ExceptionUtil;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.Version;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Map;

public class TestBundle implements Bundle {

    static final long TEST_BUNDLE_ID = 1L;

    private final TestBundleContext testBundleContext;
    private final Activator activator;
    private volatile int state;

    public TestBundle() {
        this(null);
    }

    public TestBundle(RegisterDeregisterListener registerDeregisterListener) {
        this.testBundleContext = new TestBundleContext(this, registerDeregisterListener);
        this.activator = new Activator();
        setState(RESOLVED);
    }

    @Override
    public TestBundleContext getBundleContext() {
        return testBundleContext;
    }

    @Override
    public int getState() {
        return state;
    }

    void setState(int state) {
        this.state = state;
    }

    @Override
    public long getBundleId() {
        return TEST_BUNDLE_ID;
    }

    @Override
    public URL getResource(String name) {
        return getClass().getResource(name);
    }

    @Override
    public Enumeration getResources(String name) throws IOException {
        return getClass().getClassLoader().getResources(name);
    }

    @Override
    public Enumeration findEntries(String path, String filePattern, boolean recursive) {
        return null;
    }

    @Override
    public Class loadClass(String name) throws ClassNotFoundException {
        return Class.forName(name);
    }

    @Override
    public String getSymbolicName() {
        return "com.hazelcast";
    }

    @Override
    public ServiceReference[] getRegisteredServices() {
        return testBundleContext.getAllServiceReferences();
    }

    @Override
    public synchronized void start() throws BundleException {
        int currentState = state;
        if (state == RESOLVED) {
            try {
                setState(STARTING);
                activator.start(testBundleContext);
                setState(ACTIVE);
            } catch (Throwable t) {
                setState(currentState);
                ExceptionUtil.rethrow(t);
            }
        }
    }

    @Override
    public synchronized void stop() throws BundleException {
        int currentState = state;
        if (state == ACTIVE) {
            try {
                setState(STOPPING);
                activator.stop(testBundleContext);
                setState(RESOLVED);
            } catch (Throwable t) {
                setState(currentState);
                ExceptionUtil.rethrow(t);
            }
        }
    }

    @Override
    public ServiceReference[] getServicesInUse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(int options) throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop(int options) throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(InputStream input) throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update() throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void uninstall() throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Dictionary getHeaders() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLocation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPermission(Object permission) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Dictionary getHeaders(String locale) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Enumeration getEntryPaths(String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getEntry(String path) {
        return getClass().getResource(path);
    }

    @Override
    public long getLastModified() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map getSignerCertificates(int signersType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Version getVersion() {
        throw new UnsupportedOperationException();
    }

    interface RegisterDeregisterListener {

        void onRegister(String clazz, TestServiceReference serviceReference);

        void onDeregister(String clazz, TestServiceReference serviceReference);
    }
}
