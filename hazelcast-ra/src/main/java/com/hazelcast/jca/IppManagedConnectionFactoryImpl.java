package com.hazelcast.jca;

import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.security.auth.Subject;

public class IppManagedConnectionFactoryImpl extends ManagedConnectionFactoryImpl
{
   private static final long serialVersionUID = 1L;
   
   public static final String PRP_CONNECTION_TRACING_EVENTS = "Infinity.Engine.Caching.Hazelcast.RaConnectionTracing.Events";

   public static final String PRP_CONNECTION_TRACING_CALL_STACK = "Infinity.Engine.Caching.Hazelcast.RaConnectionTracing.CallStack";
   
   private final Set<HzConnectionEvent> hzConnectionTracingEvents;
   
   private final boolean hzConnectionTracingDetail;
   
   public IppManagedConnectionFactoryImpl()
   {
      String tracingSpec = System.getProperty(PRP_CONNECTION_TRACING_EVENTS, "");
      if ((null != tracingSpec) && (0 < tracingSpec.length()))
      {
         List<HzConnectionEvent> traceEvents = new ArrayList<HzConnectionEvent>();
         traceEvents.add(HzConnectionEvent.FACTORY_INIT);

         for (String traceEventId : tracingSpec.split(","))
         {
            traceEventId = traceEventId.trim();
            try
            {
               HzConnectionEvent traceEvent = HzConnectionEvent.valueOf(traceEventId);
               if (null != traceEvent)
               {
                  traceEvents.add(traceEvent);
               }
            }
            catch (IllegalArgumentException iae)
            {
               System.out.println("Ignoring illegal token \"" + traceEventId
                     + "\" from system property " + PRP_CONNECTION_TRACING_EVENTS
                     + ", valid tokens are " + EnumSet.allOf(HzConnectionEvent.class));
            }
         }

         this.hzConnectionTracingEvents = EnumSet.copyOf(traceEvents);
      }
      else
      {
         this.hzConnectionTracingEvents = emptySet();
      }

      String tracingDetail = System.getProperty(PRP_CONNECTION_TRACING_CALL_STACK, "false");
      this.hzConnectionTracingDetail = ((null != tracingDetail) && "true".equals(tracingDetail));
      
      logHzConnectionEvent(this, HzConnectionEvent.FACTORY_INIT);
   }

   public void logHzConnectionEvent(Object eventSource, HzConnectionEvent event)
   {
      if (hzConnectionTracingEvents.contains(event))
      {
         System.out.print("HZ Connection Event <<" + event + ">> for " + eventSource
               + " in thread [" + Thread.currentThread().getName() + "]");

         if (hzConnectionTracingDetail)
         {
            System.out.print(", ");
            new Exception("Hz Connection Event Call Stack").printStackTrace(System.out);
         }
         else
         {
            System.out.println();
         }
      }
   }
   
   @Override
   public ManagedConnection createManagedConnection(Subject subject,
         ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      log(this, "createManagedConnection (IPP)");

      return new IppManagedConnectionImpl(cxRequestInfo, this);
   }

   @Override
   @SuppressWarnings({"unchecked", "rawtypes"})
   public ManagedConnection matchManagedConnections(Set connectionSet, Subject subject,
         ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      log(this, "matchManagedConnections (IPP)");
      
      if ((null != connectionSet) && !connectionSet.isEmpty())
      {
         for (ManagedConnection conn : (Set<ManagedConnection>) connectionSet)
         {
            if (conn instanceof IppManagedConnectionImpl)
            {
               ConnectionRequestInfo otherCxRequestInfo = ((IppManagedConnectionImpl) conn).getCxRequestInfo();
               if (((null == otherCxRequestInfo) && (null == cxRequestInfo))
                     || otherCxRequestInfo.equals(cxRequestInfo))
               {
                  return conn;
               }
            }
         }
      }
      
      return super.matchManagedConnections(connectionSet, subject, cxRequestInfo);
   }
   
   @Override
   public boolean equals(Object obj)
   {
      // overriding this is required as of RA spec
      return super.equals(obj);
   }
   
   @Override
   public int hashCode()
   {
      // overriding this is required as of RA spec
      return super.hashCode();
   }

   public static enum HzConnectionEvent
   {
      FACTORY_INIT, CREATE, TX_START, TX_COMPLETE, CLEANUP, DESTROY,
   }
}
