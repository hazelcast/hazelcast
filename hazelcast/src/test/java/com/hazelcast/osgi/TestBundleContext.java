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

    private final AtomicInteger COUNTER = new AtomicInteger();
    private final Object MUTEX = new Object();

    private final TestBundle testBundle;
    private final Map<String, List<ServiceReference>> serviceReferenceMap =
            new HashMap<String, List<ServiceReference>>();
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
                new TestServiceReference(testBundle, service, COUNTER.incrementAndGet());
        for (String clazz : clazzes) {
            registerServiceInternal(clazz, serviceReference);
        }
        return new TestServiceRegistration(serviceReference);
    }

    @Override
    public ServiceRegistration registerService(String clazz, Object service, Dictionary properties) {
        TestServiceReference serviceReference =
                new TestServiceReference(testBundle, service, COUNTER.incrementAndGet());
        registerServiceInternal(clazz, serviceReference);
        return new TestServiceRegistration(serviceReference);
    }

    private void registerServiceInternal(String clazz, TestServiceReference serviceReference) {
        synchronized (MUTEX) {
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
        // We simply ignore filter since we don't use filer in our tests
        synchronized (MUTEX) {
            List<ServiceReference> serviceReferences = serviceReferenceMap.get(clazz);
            if (serviceReferences != null && !serviceReferences.isEmpty()) {
                return serviceReferences.toArray(new ServiceReference[serviceReferences.size()]);
            }
        }
        return null;
    }

    @Override
    public ServiceReference[] getAllServiceReferences(String clazz, String filter) throws InvalidSyntaxException {
        // We simply ignore filter since we don't use filer in our tests
        synchronized (MUTEX) {
            List<ServiceReference> serviceReferences = serviceReferenceMap.get(clazz);
            if (serviceReferences != null && !serviceReferences.isEmpty()) {
                return serviceReferences.toArray(new ServiceReference[serviceReferences.size()]);
            }
        }
        return null;
    }

    ServiceReference[] getAllServiceReferences() {
        List<ServiceReference> allServiceReferences = new ArrayList<ServiceReference>();
        // We simply ignore filter since we don't use filer in our tests
        synchronized (MUTEX) {
            for (Map.Entry<String, List<ServiceReference>> entry : serviceReferenceMap.entrySet()) {
                List<ServiceReference> serviceReferences = entry.getValue();
                allServiceReferences.addAll(serviceReferences);
            }
        }
        return allServiceReferences.toArray(new ServiceReference[allServiceReferences.size()]);
    }

    @Override
    public ServiceReference getServiceReference(String clazz) {
        synchronized (MUTEX) {
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
            synchronized (MUTEX) {
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
