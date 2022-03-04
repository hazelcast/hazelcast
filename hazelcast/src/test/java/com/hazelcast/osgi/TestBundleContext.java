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

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.BundleListener;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkListener;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class TestBundleContext implements BundleContext {

    private final AtomicInteger counter = new AtomicInteger();
    private final Object mutex = new Object();

    private final Map<String, List<ServiceReference>> serviceReferenceMap = new HashMap<String, List<ServiceReference>>();

    private final TestBundle testBundle;
    private final TestBundle.RegisterDeregisterListener registerDeregisterListener;

    TestBundleContext(TestBundle testBundle) {
        this.testBundle = testBundle;
        this.registerDeregisterListener = null;
    }

    TestBundleContext(TestBundle testBundle, TestBundle.RegisterDeregisterListener registerDeregisterListener) {
        this.testBundle = testBundle;
        this.registerDeregisterListener = registerDeregisterListener;
    }

    @Override
    public TestBundle getBundle() {
        return testBundle;
    }

    @Override
    public Bundle getBundle(long id) {
        if (id == testBundle.getBundleId()) {
            return testBundle;
        }
        return null;
    }

    @Override
    public Bundle[] getBundles() {
        return new Bundle[]{testBundle};
    }

    @Override
    public ServiceRegistration registerService(String[] clazzes, Object service, Dictionary properties) {
        TestServiceReference serviceReference =
                new TestServiceReference(testBundle, service, counter.incrementAndGet());
        for (String clazz : clazzes) {
            registerServiceInternal(clazz, serviceReference);
        }
        return new TestServiceRegistration(serviceReference);
    }

    @Override
    public ServiceRegistration registerService(String clazz, Object service, Dictionary properties) {
        TestServiceReference serviceReference =
                new TestServiceReference(testBundle, service, counter.incrementAndGet());
        registerServiceInternal(clazz, serviceReference);
        return new TestServiceRegistration(serviceReference);
    }

    private void registerServiceInternal(String clazz, TestServiceReference serviceReference) {
        synchronized (mutex) {
            List<ServiceReference> serviceReferences = serviceReferenceMap.get(clazz);
            if (serviceReferences == null) {
                serviceReferences = new ArrayList<ServiceReference>();
                serviceReferenceMap.put(clazz, serviceReferences);
            }
            serviceReferences.add(serviceReference);
            if (registerDeregisterListener != null) {
                registerDeregisterListener.onRegister(clazz, serviceReference);
            }
        }
    }

    @Override
    public ServiceReference[] getServiceReferences(String clazz, String filter) throws InvalidSyntaxException {
        // we simply ignore filter since we don't use filer in our tests
        synchronized (mutex) {
            List<ServiceReference> serviceReferences = serviceReferenceMap.get(clazz);
            if (serviceReferences != null && !serviceReferences.isEmpty()) {
                return serviceReferences.toArray(new ServiceReference[0]);
            }
        }
        return null;
    }

    @Override
    public ServiceReference[] getAllServiceReferences(String clazz, String filter) throws InvalidSyntaxException {
        // we simply ignore filter since we don't use filer in our tests
        synchronized (mutex) {
            List<ServiceReference> serviceReferences = serviceReferenceMap.get(clazz);
            if (serviceReferences != null && !serviceReferences.isEmpty()) {
                return serviceReferences.toArray(new ServiceReference[0]);
            }
        }
        return null;
    }

    ServiceReference[] getAllServiceReferences() {
        List<ServiceReference> allServiceReferences = new ArrayList<ServiceReference>();
        // we simply ignore filter since we don't use filer in our tests
        synchronized (mutex) {
            for (Map.Entry<String, List<ServiceReference>> entry : serviceReferenceMap.entrySet()) {
                List<ServiceReference> serviceReferences = entry.getValue();
                allServiceReferences.addAll(serviceReferences);
            }
        }
        return allServiceReferences.toArray(new ServiceReference[0]);
    }

    @Override
    public ServiceReference getServiceReference(String clazz) {
        synchronized (mutex) {
            List<ServiceReference> serviceReferences = serviceReferenceMap.get(clazz);
            if (serviceReferences == null || serviceReferences.isEmpty()) {
                return null;
            } else {
                /*
                 * In fact, this is not sync with spec since spec says that:
                 *      =============================================================================================
                 *      If multiple such services exist, the service with the highest ranking (as
                 *      specified in its {@link org.osgi.framework.Constants#SERVICE_RANKING} property) is returned.
                 *
                 *      If there is a tie in ranking, the service with the lowest service ID (as
                 *      specified in its {@link org.osgi.framework.Constants#SERVICE_ID} property); that is, the
                 *      service that was registered first is returned.
                 *      =============================================================================================
                 */
                return serviceReferences.get(0);
            }
        }
    }

    @Override
    public Object getService(ServiceReference reference) {
        if (reference instanceof TestServiceReference) {
            return ((TestServiceReference) reference).getService();
        } else {
            throw new IllegalArgumentException("Only `TestServiceReference` instances are supported!");
        }
    }

    @Override
    public boolean ungetService(ServiceReference reference) {
        if (reference instanceof TestServiceReference) {
            synchronized (mutex) {
                boolean removed = false;
                for (Map.Entry<String, List<ServiceReference>> entry : serviceReferenceMap.entrySet()) {
                    List<ServiceReference> serviceReferences = entry.getValue();
                    if (serviceReferences.remove(reference)) {
                        removed = true;
                        if (registerDeregisterListener != null) {
                            registerDeregisterListener.onDeregister(entry.getKey(), (TestServiceReference) reference);
                        }
                    }
                }
                return removed;
            }
        } else {
            throw new IllegalArgumentException("Only `TestServiceReference` instances are supported!");
        }
    }

    @Override
    public String getProperty(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bundle installBundle(String location, InputStream input) throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bundle installBundle(String location) throws BundleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addServiceListener(ServiceListener listener, String filter) throws InvalidSyntaxException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addServiceListener(ServiceListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeServiceListener(ServiceListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBundleListener(BundleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeBundleListener(BundleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFrameworkListener(FrameworkListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeFrameworkListener(FrameworkListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public File getDataFile(String filename) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Filter createFilter(String filter) throws InvalidSyntaxException {
        throw new UnsupportedOperationException();
    }
}
