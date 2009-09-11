/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.jmx;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;

/**
 * A generic class to instrument object with dynamic MBeans.
 * 
 * Implements the method instrument(), in which add attributes and
 * operation exposed to JMX.
 * 
 * If the managed object is an immutable state of some other object that requires
 *  a special refresh strategy, override method {@link #refresh}
 * 
 * http://docs.sun.com/app/docs/doc/816-4178/6madjde4n?a=view
 *
 * @author Marco Ferrante, DISI - University of Genoa
 */
public abstract class AbstractMBean<E> implements DynamicMBean, MBeanRegistration {

	protected final static Logger logger = Logger.getLogger(AbstractMBean.class.getName());

	protected transient MBeanServer mbeanServer;

	private volatile ObjectNameSpec parentName = new ObjectNameSpec();
	private volatile ObjectName objectName;
	
	// Use a weak reference? http://weblogs.java.net/blog/emcmanus/archive/2005/07/cleaning_up_an_1.html
	private E managedObject;

	private String description;

	/**
	 * Attribute infos
	 */
	private class AttributeInfo {
		protected String name;
		protected String description = null;
		protected Method getter = null;
		protected Method setter = null;

		public AttributeInfo(String name) {
			this.name = name;
		}
		
		public MBeanAttributeInfo getInfo() throws IntrospectionException {
			return new MBeanAttributeInfo(name, description, getter, setter);
		}
	}

	/**
	 * Attribute infos
	 */
	private class OperationInfo {
		protected transient String name;
		protected transient String description;
		protected transient Method method;
		protected MBeanOperationInfo info;

		public OperationInfo(String name) {
			this.name = name;
		}
		
		public OperationInfo(String description, Method method)
				throws IntrospectionException {
			info = new MBeanOperationInfo(description, method);
			this.method = method;
		}

		public MBeanOperationInfo getInfo() throws IntrospectionException {
			if (info == null) {
				info = new MBeanOperationInfo(description, method);
			}
			return info;
		}
	}

	private HashMap<String, AttributeInfo> attributeInfos
	= new HashMap<String, AttributeInfo>();
	/**
	 * Operation infos
	 */
	private HashMap<String, OperationInfo> operationInfos
	= new HashMap<String, OperationInfo>();

	public AbstractMBean(E managedObject) {
		this.managedObject = managedObject;
	}

	public final E getManagedObject() {
		return managedObject;
	}

	/**
	 * Override this method if the managed object requires some refresh
	 * before reading. Return the new state/object.
	 */
	protected E refresh() {
		return getManagedObject();
	}

	/**
	 * Prepare MBean information, invoked by preRegister().
	 * 
	 * @throws Exception
	 */
	private final void instrument() throws Exception {

		// Class description
		JMXDescription dscr = this.getClass().getAnnotation(JMXDescription.class);
		if (dscr != null) {
			this.description = dscr.value();
		}
		
		// Search for annotations
		for (Method method : getClass().getMethods()) {
			
			// Attributes
			if (method.isAnnotationPresent(JMXAttribute.class)) {
				JMXAttribute annotation = method.getAnnotation(JMXAttribute.class);
				if (logger.isLoggable(Level.FINEST)) {
					logger.finest("Found annotation " + annotation
							+ " in method " + method.getName()
							+ " in object " + hashCode()
							+ " of class " + getClass().getName());
				}
				
				String name = annotation.value();
				if (name.length() == 0) {
					String methodName = method.getName();
					if (methodName.startsWith("get")) {
						name = methodName.substring(3);
					}
					else if (methodName.startsWith("set")) {
						name = methodName.substring(3);
					}
					else if (methodName.startsWith("is")) {
						name = methodName.substring(2);
					}
					else {
						logger.warning("Uncomplaint method name " + method.getName() + " for attribute");
						name = method.getName();
					}
				}
				
				AttributeInfo info = attributeInfos.get(name);
				if (info == null) {
					info = new AttributeInfo(name);
					attributeInfos.put(name, info);
				}
				
				// Attribute description
				dscr = method.getAnnotation(JMXDescription.class);
				if (dscr != null) {
					if (info.description != null) {
						logger.warning("Duplicate description for attribute " + name + ", overwrite");
					}
					info.description = dscr.value();
				}
				
				// getter
				if (method.getReturnType() != Void.class && method.getParameterTypes().length == 0) {
					if (info.getter != null) {
						throw new IllegalArgumentException("Duplicate getter for attribute " + name
								+ " in class " + getClass().getName());
					}
					else {
						info.getter = method;
					}
				}
				// setter
				else if (method.getReturnType() == Void.class && method.getParameterTypes().length == 1) {
					if (info.setter != null) {
						throw new IllegalArgumentException("Duplicate setter for attribute "
								+ name + " in class " + getClass().getName());
					}
					else {
						info.setter = method;
					}
				}
				else {
					logger.warning("Method " + method.getName() + " is neither a setter or a getter");
				}
			}
			
			// Operations
			if (method.isAnnotationPresent(JMXOperation.class)) {
				JMXOperation annotation = method.getAnnotation(JMXOperation.class);
				if (logger.isLoggable(Level.FINEST)) {
					logger.finest("Found operation annotation " + annotation);
				}
				
				String name = annotation.value();
				if (name.length() == 0) {
					throw new IllegalArgumentException("Empty operation name in  " + method.getName()
							+ " in class " + getClass().getName());
				}
				
				if (operationInfos.containsKey(name)) {
					throw new IllegalArgumentException("Duplicate operation " + name
							+ " in class " + getClass().getName());
				}

				OperationInfo info = new OperationInfo(name);
				operationInfos.put(name, info);
				
				// Attribute description
				dscr = method.getAnnotation(JMXDescription.class);
				if (dscr != null) {
					info.description = dscr.value();
				}
				info.method = method;
			}
		}
	}
	
	/**
	 * From DynamicMBean interface
	 */
	public MBeanInfo getMBeanInfo() {
		final MBeanInfo mbeanInfo =
			new MBeanInfo(
					managedObject.getClass().getName(),
					description,
					buildAttributeInfos(),
					null,
					buildOperationInfos(),
					null
			);
		return mbeanInfo;
	}	

	private Object getValue(String attribute, boolean refresh)
			throws AttributeNotFoundException, MBeanException, ReflectionException {
		if (attribute == null || attribute.length() == 0)
			throw new NullPointerException("Invalid null attribute requested");

		AttributeInfo info = attributeInfos.get(attribute);
		if (info == null) {
			logger.log(Level.WARNING, "Managed attribute " + attribute + " not registered in MBean");
			throw new AttributeNotFoundException("Attribute " + attribute + " not registered in MBean");
		}

		if (refresh) {
			managedObject = refresh();
		}

		Object result;
		try {
			Method getter = info.getter;
			if (getter.getDeclaringClass() == this.getClass()) {
				if (logger.isLoggable(Level.FINEST)) {
					logger.log(Level.FINEST, "Attribute '" + attribute + "' belonging to MBean");
				}
				result = getter.invoke(this);
			}
			else {
				if (logger.isLoggable(Level.FINEST)) {
					logger.log(Level.FINEST, "Attribute '" + attribute + "' belonging to managed object");
				}
				result = getter.invoke(managedObject);
			}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Error accessing attribute " + attribute, e);
			throw new ReflectionException(e);
		}

		return result;
	}

	/**
	 * Local attribute override managed object attribute
	 */
	public final Object getAttribute(String attribute)
	throws AttributeNotFoundException, MBeanException, ReflectionException {
		return getValue(attribute, true);
	}

	/**
	 * getAttributes() in interface DynamicMBean
	 * 
	 * @param attributes A String array of names of the attributes to be retrieved.
	 * @return The array of the retrieved attributes.
	 * @exception RuntimeOperationsException Wraps an
	 * {@link IllegalArgumentException}: The object name in parameter is
	 * null or attributes in parameter is null.
	 */
	public final AttributeList getAttributes(String[] attributes) {
		managedObject = refresh();

		AttributeList result = new AttributeList(attributes.length);
		try {
			for (String name : attributes) {
				Object value = getValue(name, false);
				Attribute attribute = new Attribute(name, value);
				result.add(attribute);
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}

		return result;
	}

	/**
	 * invoke() in interface DynamicMBean
	 */
	public final Object invoke(String actionName, Object[] params, String[] signature)
	throws MBeanException, ReflectionException {
		if (actionName == null || actionName.length() == 0)
			throw new NullPointerException("Invalid null operation invoked");

		OperationInfo info = operationInfos.get(actionName);
		if (info == null) {
			logger.log(Level.WARNING, "Managed operation " + actionName + " not registered in MBean");
			throw new UnsupportedOperationException("Operation " + actionName + " not registered in MBean");
		}

		Object result;
		try {
			Method method = info.method;
			if (method.getDeclaringClass() == this.getClass()) {
				if (logger.isLoggable(Level.FINEST)) {
					logger.log(Level.FINEST, "Operation '" + actionName + "' belonging to MBean");
				}
				result = method.invoke(this);
			}
			else {
				if (logger.isLoggable(Level.FINEST)) {
					logger.log(Level.FINEST, "Operation '" + actionName + "' belonging to managed object", actionName);
				}
				result = method.invoke(managedObject);
			}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Error invoking operation " + actionName, e);
			throw new ReflectionException(e);
		}

		return result;
	}

	public final void setAttribute(Attribute attribute)
	throws AttributeNotFoundException, InvalidAttributeValueException,
	MBeanException, ReflectionException {
		// Required by DynamicMBean interface, nothing to do
		throw new UnsupportedOperationException();
	}

	public final AttributeList setAttributes(AttributeList attributes) {
		// Required by DynamicMBean interface, nothing to do
		throw new UnsupportedOperationException();
	}	

	private MBeanAttributeInfo[] buildAttributeInfos() {
		if (attributeInfos == null || attributeInfos.size() == 0) {
			return null;
		}
		MBeanAttributeInfo[] result = new MBeanAttributeInfo[attributeInfos.size()];
		int i = 0;
		for (AttributeInfo info : attributeInfos.values()) {
			try {
				result[i++] = info.getInfo();
			} catch (IntrospectionException e) {
				logger.log(Level.WARNING, "Error building attribute list", e);
				throw new IllegalArgumentException(e);
			}
		}
		return result;
	}

	private MBeanOperationInfo[] buildOperationInfos() {
		if (operationInfos == null || operationInfos.size() == 0) {
			return null;
		}
		MBeanOperationInfo[] result = new MBeanOperationInfo[operationInfos.size()];
		int i = 0;
		for (OperationInfo info : operationInfos.values()) {
			try {
				result[i++] = info.getInfo();
			} catch (IntrospectionException e) {
				logger.log(Level.WARNING, "Error building operation list", e);
				throw new IllegalArgumentException(e);
			}
		}
		return result;
	}

	public void setParentName(ObjectNameSpec spec) {
		parentName = spec;
	}
	
	public ObjectNameSpec getParentName() {
		return parentName;
	}
	
	/**
	 * Override to provide a JMX name
	 */
	protected ObjectNameSpec getNameSpec() {
		return parentName.getNested("unknown", "@" + hashCode());
	}
	
	/**
	 * The current objectName.
	 * To provide a default, override this method and pass null as name 
	 * to the method MBeanServer.registerMBean()
	 * 
	 * @return The current objectName
	 */
	public final ObjectName getObjectName() throws Exception {
		if (objectName == null) {
			objectName = getNameSpec().buildObjectName();
		}
		return objectName;
	}

	/**
	 * Build the current objectName from the spec.
	 * 
	 * @return The new objectName
	 */
//	public ObjectName getObjectName(ObjectNameSpec spec) throws Exception {
//		objectName = spec.buildObjectName();
//		return objectName;
//	}

	/**
	 * From interface {@link javax.management.MBeanRegistration}
	 */
	public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
		try {
			instrument();
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Error generating MBeanInfo", e);
			throw e;
		}
		
		if (name != null) {
			objectName = name;
		}
		mbeanServer = server;

		return getObjectName();
	}

	/**
	 * From interface {@link javax.management.MBeanRegistration}
	 */
	public void postRegister(Boolean registrationDone) {
		;  // Nothing to do
	}

	/**
	 * From interface {@link javax.management.MBeanRegistration}
	 */
	public void preDeregister() throws Exception {
		;  // Nothing to do
	}

	/**
	 * From interface {@link javax.management.MBeanRegistration}
	 */
	public void postDeregister() {
		; // Nothing to do
	}

}
