/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.web.tomcat;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionActivationListener;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;
import javax.servlet.http.HttpSessionEvent;

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.SessionListener;
import org.apache.catalina.security.SecurityUtil;
import org.apache.catalina.session.StandardSession;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.query.SqlPredicate;

/**
 * @author ali
 * TODO there are still not overwritten methods which has reference to attributes (setId)
 */

public class HazelSession extends StandardSession {
	

    /**
     * Construct a new Session associated with the specified Manager.
     *
     * @param manager The manager with which this Session is associated
     */
	public HazelSession(Manager manager) {
		super(manager);
	}
	
    /**
     * Descriptive information describing this Session implementation.
     */
    protected static final String info = "HazelSession/1.0";
    

    /**
     * Return the <code>HttpSession</code> for which this object
     * is the facade.
     */
    public HttpSession getSession() {

        if (facade == null){
            if (SecurityUtil.isPackageProtectionEnabled()){
                final HazelSession fsession = this;
                facade = AccessController.doPrivileged(
                        new PrivilegedAction<HazelSessionFacade>(){
                    public HazelSessionFacade run(){
                        return new HazelSessionFacade(fsession);
                    }
                });
            } else {
                facade = new HazelSessionFacade(this);
            }
        }
        return (facade);

    }


    /**
     * Return the object bound with the specified name in this session, or
     * <code>null</code> if no object is bound with that name.
     *
     * @param name Name of the attribute to be returned
     *
     * @exception IllegalStateException if this method is called on an
     *  invalidated session
     */
    public Object getAttribute(String name) {

        if (!isValidInternal()){
        	throw new IllegalStateException
        	(sm.getString("standardSession.getAttribute.ise"));
        }

        if (name == null){
        	return null;
        }
        
        HazelAttribute hattribute = (HazelAttribute)attributes.get(name);
        
        if(hattribute == null){
        	hattribute = (HazelAttribute) Hazelcast.getMap("attributes").get(getIdInternal() + "_" + name);
        	if(hattribute == null){
        		attributes.put(name, new HazelAttribute(getIdInternal(), name, null));
        		return null;
        	}
        }
        long requestId = HazelValve.requestLocal.get();
        hattribute.touch(requestId);
        return hattribute.getValue();
    }
    
    
    /**
     * Get HazelAttribute directly
     */
    public Object getLocalAttribute(String name) {
        return super.getAttribute(name);
    }
    

    /**
     * Bind an object to this session, using the specified name.  If an object
     * of the same name is already bound to this session, the object is
     * replaced.
     * <p>
     * After this method executes, and if the object implements
     * <code>HttpSessionBindingListener</code>, the container calls
     * <code>valueBound()</code> on the object.
     *
     * @param name Name to which the object is bound, cannot be null
     * @param value Object to be bound, cannot be null
     * @param notify whether to notify session listeners
     * @exception IllegalArgumentException if an attempt is made to add a
     *  non-serializable object in an environment marked distributable.
     * @exception IllegalStateException if this method is called on an
     *  invalidated session
     */

    public void setAttribute(String name, Object value, boolean notify) {

        // Name cannot be null
        if (name == null)
            throw new IllegalArgumentException
                (sm.getString("standardSession.setAttribute.namenull"));

        // Null value is the same as removeAttribute()
        if (value == null) {
            removeAttribute(name);
            return;
        }

        // Validate our current state
        if (!isValidInternal())
            throw new IllegalStateException(sm.getString(
                    "standardSession.setAttribute.ise", getIdInternal()));
        if ((manager != null) && manager.getDistributable() &&
          !(value instanceof Serializable))
            throw new IllegalArgumentException
                (sm.getString("standardSession.setAttribute.iae", name));
        
        HazelAttribute oldHattribute = (HazelAttribute)attributes.get(name);
        
        // Construct an event with the new value
        HttpSessionBindingEvent event = null;

        // Call the valueBound() method if necessary
        if (notify && value instanceof HttpSessionBindingListener) {
            // Don't call any notification if replacing with the same value
            if (oldHattribute != null && value != oldHattribute.getValue()) {
                event = new HttpSessionBindingEvent(getSession(), name, value);
                try {
                    ((HttpSessionBindingListener) value).valueBound(event);
                } catch (Throwable t){
                    manager.getContainer().getLogger().error
                    (sm.getString("standardSession.bindingEvent"), t); 
                }
            }
        }
        

        // Replace or add this attribute
        if(oldHattribute == null){
        	oldHattribute = new HazelAttribute(getIdInternal(), name, value);
        }
        else{
        	oldHattribute.setValue(value);
        }
        long requestId = HazelValve.requestLocal.get();
        oldHattribute.touch(requestId);
        HazelAttribute unboundHattribute = (HazelAttribute)attributes.put(name, oldHattribute);
        Object unbound = unboundHattribute != null ? unboundHattribute.getValue() : null;

        // Call the valueUnbound() method if necessary
        if (notify && (unbound != null) && (unbound != value) &&
            (unbound instanceof HttpSessionBindingListener)) {
            try {
                ((HttpSessionBindingListener) unbound).valueUnbound
                    (new HttpSessionBindingEvent(getSession(), name));
            } catch (Throwable t) {
                ExceptionUtils.handleThrowable(t);
                manager.getContainer().getLogger().error
                    (sm.getString("standardSession.bindingEvent"), t);
            }
        }
        
        if ( !notify ) return;
        
        // Notify interested application event listeners
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationEventListeners();
        if (listeners == null)
            return;
        for (int i = 0; i < listeners.length; i++) {
            if (!(listeners[i] instanceof HttpSessionAttributeListener))
                continue;
            HttpSessionAttributeListener listener =
                (HttpSessionAttributeListener) listeners[i];
            try {
                if (unbound != null) {
                    fireContainerEvent(context,
                                       "beforeSessionAttributeReplaced",
                                       listener);
                    if (event == null) {
                        event = new HttpSessionBindingEvent
                            (getSession(), name, unbound);
                    }
                    listener.attributeReplaced(event);
                    fireContainerEvent(context,
                                       "afterSessionAttributeReplaced",
                                       listener);
                } else {
                    fireContainerEvent(context,
                                       "beforeSessionAttributeAdded",
                                       listener);
                    if (event == null) {
                        event = new HttpSessionBindingEvent
                            (getSession(), name, value);
                    }
                    listener.attributeAdded(event);
                    fireContainerEvent(context,
                                       "afterSessionAttributeAdded",
                                       listener);
                }
            } catch (Throwable t) {
                ExceptionUtils.handleThrowable(t);
                try {
                    if (unbound != null) {
                        fireContainerEvent(context,
                                           "afterSessionAttributeReplaced",
                                           listener);
                    } else {
                        fireContainerEvent(context,
                                           "afterSessionAttributeAdded",
                                           listener);
                    }
                } catch (Exception e) {
                    // Ignore
                }
                manager.getContainer().getLogger().error
                    (sm.getString("standardSession.attributeEvent"), t);
            }
        }

    }
    
    /**
     * Remove the object bound with the specified name from this session.  If
     * the session does not have an object bound with this name, this method
     * does nothing.
     * <p>
     * After this method executes, and if the object implements
     * <code>HttpSessionBindingListener</code>, the container calls
     * <code>valueUnbound()</code> on the object.
     *
     * @param name Name of the object to remove from this session.
     * @param notify Should we notify interested listeners that this
     *  attribute is being removed?
     */
    protected void removeAttributeInternal(String name, boolean notify) {

        // Avoid NPE
        if (name == null) return;

        // Remove this attribute from our collection
        HazelAttribute hattribute = (HazelAttribute)attributes.get(name);
        if(hattribute == null || hattribute.getValue() == null){
        	return;
        }
        
        Object value = hattribute.getValue();
        hattribute.setValue(null);
        long requestId = HazelValve.requestLocal.get();
        hattribute.touch(requestId);
        attributes.put(name, hattribute);

        // Do we need to do valueUnbound() and attributeRemoved() notification?
        if (!notify || (value == null)) {
            return;
        }

        // Call the valueUnbound() method if necessary
        HttpSessionBindingEvent event = null;
        if (value instanceof HttpSessionBindingListener) {
            event = new HttpSessionBindingEvent(getSession(), name, value);
            ((HttpSessionBindingListener) value).valueUnbound(event);
        }

        // Notify interested application event listeners
        Context context = (Context) manager.getContainer();
        Object listeners[] = context.getApplicationEventListeners();
        if (listeners == null)
            return;
        for (int i = 0; i < listeners.length; i++) {
            if (!(listeners[i] instanceof HttpSessionAttributeListener))
                continue;
            HttpSessionAttributeListener listener =
                (HttpSessionAttributeListener) listeners[i];
            try {
                fireContainerEvent(context,
                                   "beforeSessionAttributeRemoved",
                                   listener);
                if (event == null) {
                    event = new HttpSessionBindingEvent
                        (getSession(), name, value);
                }
                listener.attributeRemoved(event);
                fireContainerEvent(context,
                                   "afterSessionAttributeRemoved",
                                   listener);
            } catch (Throwable t) {
                ExceptionUtils.handleThrowable(t);
                try {
                    fireContainerEvent(context,
                                       "afterSessionAttributeRemoved",
                                       listener);
                } catch (Exception e) {
                    // Ignore
                }
                manager.getContainer().getLogger().error
                    (sm.getString("standardSession.attributeEvent"), t);
            }
        }

    }
    
    /**
     * Perform internal processing required to activate this
     * session.
     */
    public void activate() {

        // Initialize access count
        if (ACTIVITY_CHECK) {
            accessCount = new AtomicInteger();
        }
        
        // Notify interested session event listeners
        fireSessionEvent(Session.SESSION_ACTIVATED_EVENT, null);

        // Notify ActivationListeners
        HttpSessionEvent event = null;
        String keys[] = keys();
        for (int i = 0; i < keys.length; i++) {
            Object attribute = ((HazelAttribute)attributes.get(keys[i])).getValue();
            if (attribute instanceof HttpSessionActivationListener) {
                if (event == null)
                    event = new HttpSessionEvent(getSession());
                try {
                    ((HttpSessionActivationListener)attribute)
                        .sessionDidActivate(event);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    manager.getContainer().getLogger().error
                        (sm.getString("standardSession.attributeEvent"), t);
                }
            }
        }

    }
    
    /**
     * Perform the internal processing required to passivate
     * this session.
     */
    public void passivate() {

        // Notify interested session event listeners
        fireSessionEvent(Session.SESSION_PASSIVATED_EVENT, null);

        // Notify ActivationListeners
        HttpSessionEvent event = null;
        String keys[] = keys();
        for (int i = 0; i < keys.length; i++) {
        	Object attribute = ((HazelAttribute)attributes.get(keys[i])).getValue();
            if (attribute instanceof HttpSessionActivationListener) {
                if (event == null)
                    event = new HttpSessionEvent(getSession());
                try {
                    ((HttpSessionActivationListener)attribute)
                        .sessionWillPassivate(event);
                } catch (Throwable t) {
                    ExceptionUtils.handleThrowable(t);
                    manager.getContainer().getLogger().error
                        (sm.getString("standardSession.attributeEvent"), t);
                }
            }
        }

    }
    
    /**
     * Read a serialized version of this session object from the specified
     * object input stream.
     * <p>
     * <b>IMPLEMENTATION NOTE</b>:  The reference to the owning Manager
     * is not restored by this method, and must be set explicitly.
     *
     * @param stream The input stream to read from
     *
     * @exception ClassNotFoundException if an unknown class is specified
     * @exception IOException if an input/output error occurs
     */
    protected void readObject(ObjectInputStream stream)
        throws ClassNotFoundException, IOException {

        // Deserialize the scalar instance variables (except Manager)
        authType = null;        // Transient only
        creationTime = ((Long) stream.readObject()).longValue();
        lastAccessedTime = ((Long) stream.readObject()).longValue();
        maxInactiveInterval = ((Integer) stream.readObject()).intValue();
        isNew = ((Boolean) stream.readObject()).booleanValue();
        isValid = ((Boolean) stream.readObject()).booleanValue();
        thisAccessedTime = ((Long) stream.readObject()).longValue();
        principal = null;        // Transient only
        //        setId((String) stream.readObject());
        id = (String) stream.readObject();
        if (manager.getContainer().getLogger().isDebugEnabled())
            manager.getContainer().getLogger().debug
                ("readObject() loading session " + id);

        // Deserialize the attribute count and attribute values
        if (attributes == null)
            attributes = new Hashtable<String, Object>();
        int n = ((Integer) stream.readObject()).intValue();
        boolean isValidSave = isValid;
        isValid = true;
        for (int i = 0; i < n; i++) {
            String name = (String) stream.readObject();
            Object value = stream.readObject();
            if ((value instanceof String) && (value.equals(NOT_SERIALIZED)))
                continue;
            if (manager.getContainer().getLogger().isDebugEnabled())
                manager.getContainer().getLogger().debug("  loading attribute '" + name +
                    "' with value '" + value + "'");
            attributes.put(name, new HazelAttribute(id, name, value));
        }
        isValid = isValidSave;

        if (listeners == null) {
            listeners = new ArrayList<SessionListener>();
        }

        if (notes == null) {
            notes = new Hashtable<String, Object>();
        }
    }


    /**
     * Write a serialized version of this session object to the specified
     * object output stream.
     * <p>
     * <b>IMPLEMENTATION NOTE</b>:  The owning Manager will not be stored
     * in the serialized representation of this Session.  After calling
     * <code>readObject()</code>, you must set the associated Manager
     * explicitly.
     * <p>
     * <b>IMPLEMENTATION NOTE</b>:  Any attribute that is not Serializable
     * will be unbound from the session, with appropriate actions if it
     * implements HttpSessionBindingListener.  If you do not want any such
     * attributes, be sure the <code>distributable</code> property of the
     * associated Manager is set to <code>true</code>.
     *
     * @param stream The output stream to write to
     *
     * @exception IOException if an input/output error occurs
     */
    protected void writeObject(ObjectOutputStream stream) throws IOException {

        // Write the scalar instance variables (except Manager)
        stream.writeObject(Long.valueOf(creationTime));
        stream.writeObject(Long.valueOf(lastAccessedTime));
        stream.writeObject(Integer.valueOf(maxInactiveInterval));
        stream.writeObject(Boolean.valueOf(isNew));
        stream.writeObject(Boolean.valueOf(isValid));
        stream.writeObject(Long.valueOf(thisAccessedTime));
        stream.writeObject(id);
        if (manager.getContainer().getLogger().isDebugEnabled())
            manager.getContainer().getLogger().debug
                ("writeObject() storing session " + id);

        // Accumulate the names of serializable and non-serializable attributes
        String keys[] = keys();
        ArrayList<String> saveNames = new ArrayList<String>();
        ArrayList<Object> saveValues = new ArrayList<Object>();
        for (int i = 0; i < keys.length; i++) {
            HazelAttribute hattribute = (HazelAttribute)attributes.get(keys[i]);
            if(hattribute == null)
            	continue;
            Object value = hattribute.getValue();
            
            if (value == null)
                continue;
            else if ( (value instanceof Serializable) 
                    && (!exclude(keys[i]) )) {
                saveNames.add(keys[i]);
                saveValues.add(value);
            } else {
                removeAttributeInternal(keys[i], true);
            }
        }

        // Serialize the attribute count and the Serializable attributes
        int n = saveNames.size();
        stream.writeObject(Integer.valueOf(n));
        for (int i = 0; i < n; i++) {
            stream.writeObject(saveNames.get(i));
            try {
                stream.writeObject(saveValues.get(i));
                if (manager.getContainer().getLogger().isDebugEnabled())
                    manager.getContainer().getLogger().debug
                        ("  storing attribute '" + saveNames.get(i) +
                        "' with value '" + saveValues.get(i) + "'");
            } catch (NotSerializableException e) {
                manager.getContainer().getLogger().warn
                    (sm.getString("standardSession.notSerializable",
                     saveNames.get(i), id), e);
                stream.writeObject(NOT_SERIALIZED);
                if (manager.getContainer().getLogger().isDebugEnabled())
                    manager.getContainer().getLogger().debug
                       ("  storing attribute '" + saveNames.get(i) +
                        "' with value NOT_SERIALIZED");
            }
        }

    }
    
    /**
     * Set the session identifier for this session.
     *
     * @param id The new session identifier
     */
    public void setId(String id) {

        if ((this.id != null) && (manager != null))
            manager.remove(this);

        this.id = id;

        Collection<HazelAttribute> colAttributes = Hazelcast.getMap("attributes").values(new SqlPredicate("sessionId="+id));
        if (manager != null)
            manager.add(this);
        tellNew();
    }


}
