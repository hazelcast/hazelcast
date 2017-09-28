package com.hazelcast.config;

import com.hazelcast.spi.AddressLocator;

import java.util.Properties;

public class AddressLocatorConfig {
    private boolean enabled;
    private String classname;
    private Properties properties = new Properties();
    private AddressLocator implementation;


    public boolean isEnabled() {
        return enabled;
    }

    public AddressLocatorConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassname() {
        return classname;
    }

    public AddressLocatorConfig setClassname(String classname) {
        this.classname = classname;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public AddressLocatorConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    public AddressLocator getImplementation() {
        return implementation;
    }

    public AddressLocatorConfig setImplementation(AddressLocator implementation) {
        this.implementation = implementation;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddressLocatorConfig that = (AddressLocatorConfig) o;

        if (isEnabled() != that.isEnabled()) return false;
        if (getClassname() != null ? !getClassname().equals(that.getClassname()) : that.getClassname() != null)
            return false;
        if (getProperties() != null ? !getProperties().equals(that.getProperties()) : that.getProperties() != null)
            return false;
        return getImplementation() != null ? getImplementation().equals(that.getImplementation()) : that.getImplementation() == null;
    }

    @Override
    public int hashCode() {
        int result = (isEnabled() ? 1 : 0);
        result = 31 * result + (getClassname() != null ? getClassname().hashCode() : 0);
        result = 31 * result + (getProperties() != null ? getProperties().hashCode() : 0);
        result = 31 * result + (getImplementation() != null ? getImplementation().hashCode() : 0);
        return result;
    }
}
