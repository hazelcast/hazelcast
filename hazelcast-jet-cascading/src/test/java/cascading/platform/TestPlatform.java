/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.platform;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.util.FieldTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class TestPlatform
  {
  private static final Logger LOG = LoggerFactory.getLogger( TestPlatform.class );

  public static final String CLUSTER_TESTING_PROPERTY = "test.cluster.enabled";
  public static final String PLATFORM_PREFIX = "platform.";

  private boolean useCluster = false;
  private boolean enableCluster = true;
  protected int numMappers = 0;
  protected int numReducers = 0;
  protected int numGatherPartitions = 0;

  /**
   * Method getGlobalProperties fetches all "platform." prefixed system properties.
   * <p/>
   * Sub-classes of TestPlatform should use these values as overrides before returning from
   * {@link #getProperties()}.
   *
   * @return a Map of properties
   */
  public static Map<Object, Object> getGlobalProperties()
    {
    HashMap<Object, Object> properties = new HashMap<Object, Object>();

    for( String propertyName : System.getProperties().stringPropertyNames() )
      {
      if( propertyName.startsWith( PLATFORM_PREFIX ) )
        properties.put( propertyName.substring( PLATFORM_PREFIX.length() ), System.getProperty( propertyName ) );
      }

    if( !properties.isEmpty() )
      LOG.info( "platform property overrides: ", properties );

    return properties;
    }

  protected TestPlatform()
    {
    enableCluster = Boolean.parseBoolean( System.getProperty( CLUSTER_TESTING_PROPERTY, Boolean.toString( enableCluster ) ) );
    }

  public String getName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)Platform$", "$1" ).toLowerCase();
    }

  public boolean supportsGroupByAfterMerge()
    {
    return false;
    }

  public boolean isMapReduce()
    {
    return false;
    }

  public boolean isDAG()
    {
    return false;
    }

  public int getNumMappers()
    {
    return numMappers;
    }

  public void setNumMappers( int numMappers )
    {
    this.numMappers = numMappers;
    }

  public int getNumReducers()
    {
    return numReducers;
    }

  public void setNumReducers( int numReducers )
    {
    this.numReducers = numReducers;
    }

  public int getNumGatherPartitions()
    {
    return numGatherPartitions;
    }

  public void setNumGatherPartitions( int numGatherPartitions )
    {
    this.numGatherPartitions = numGatherPartitions;
    }

  public void setNumMapTasks( Map<Object, Object> properties, int numMapTasks )
    {
    // do nothing
    }

  public void setNumReduceTasks( Map<Object, Object> properties, int numReduceTasks )
    {
    // do nothing
    }

  public void setNumGatherPartitionTasks( Map<Object, Object> properties, int numReduceTasks )
    {
    // do nothing
    }

  public Integer getNumMapTasks( Map<Object, Object> properties )
    {
    return null;
    }

  public Integer getNumReduceTasks( Map<Object, Object> properties )
    {
    return null;
    }

  public Integer getNumGatherPartitionTasks( Map<Object, Object> properties )
    {
    return null;
    }

  public abstract void setUp() throws IOException;

  public abstract Map<Object, Object> getProperties();

  public abstract void tearDown();

  public void setUseCluster( boolean useCluster )
    {
    this.useCluster = useCluster;
    }

  public boolean isUseCluster()
    {
    return enableCluster && useCluster;
    }

  public abstract void copyFromLocal( String inputFile ) throws IOException;

  public abstract void copyToLocal( String outputFile ) throws IOException;

  public abstract boolean remoteExists( String outputFile ) throws IOException;

  public abstract boolean remoteRemove( String outputFile, boolean recursive ) throws IOException;

  public abstract FlowProcess getFlowProcess();

  public abstract FlowConnector getFlowConnector( Map<Object, Object> properties );

  public FlowConnector getFlowConnector()
    {
    return getFlowConnector( getProperties() );
    }

  public abstract Tap getTap( Scheme scheme, String filename, SinkMode mode );

  public Tap getTextFile( Fields sourceFields, String filename )
    {
    return getTextFile( sourceFields, filename, SinkMode.KEEP );
    }

  public Tap getTextFile( String filename )
    {
    return getTextFile( filename, SinkMode.KEEP );
    }

  public Tap getTextFile( String filename, SinkMode mode )
    {
    return getTextFile( null, filename, mode );
    }

  public Tap getTextFile( Fields sourceFields, String filename, SinkMode mode )
    {
    return getTextFile( sourceFields, Fields.ALL, filename, mode );
    }

  public abstract Tap getTextFile( Fields sourceFields, Fields sinkFields, String filename, SinkMode mode );

  public Tap getDelimitedFile( Fields fields, String delimiter, String filename )
    {
    return getDelimitedFile( fields, false, delimiter, "\"", null, filename, SinkMode.KEEP );
    }

  public Tap getDelimitedFile( Fields fields, String delimiter, String filename, SinkMode mode )
    {
    return getDelimitedFile( fields, false, delimiter, "\"", null, filename, mode );
    }

  public Tap getTabDelimitedFile( Fields fields, String filename, SinkMode mode )
    {
    return getDelimitedFile( fields, false, "\t", "\"", null, filename, mode );
    }

  public Tap getTabDelimitedFile( Fields fields, boolean hasHeader, String filename, SinkMode mode )
    {
    return getDelimitedFile( fields, hasHeader, "\t", "\"", null, filename, mode );
    }

  public Tap getDelimitedFile( Fields fields, boolean hasHeader, String delimiter, String quote, String filename, SinkMode mode )
    {
    return getDelimitedFile( fields, hasHeader, delimiter, quote, null, filename, mode );
    }

  public Tap getDelimitedFile( Fields fields, String delimiter, String quote, String filename, SinkMode mode )
    {
    return getDelimitedFile( fields, false, delimiter, quote, null, filename, mode );
    }

  public Tap getDelimitedFile( Fields fields, String delimiter, Class[] types, String filename, SinkMode mode )
    {
    return getDelimitedFile( fields, false, delimiter, "\"", types, filename, mode );
    }

  public abstract Tap getDelimitedFile( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode );

  public abstract Tap getDelimitedFile( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode );

  public abstract Tap getDelimitedFile( String delimiter, String quote, FieldTypeResolver fieldTypeResolver, String filename, SinkMode mode );

  public abstract Tap getPartitionTap( Tap sink, Partition partition, int openThreshold );

  public abstract Scheme getTestConfigDefScheme();

  public abstract Scheme getTestFailScheme();

  public abstract Comparator getLongComparator( boolean reverseSort );

  public abstract Comparator getStringComparator( boolean reverseSort );

  public abstract String getHiddenTemporaryPath();

  protected String getApplicationJar()
    {
    // mapred.jar is for backwards compatibility with the compatibility suite
    String property = System.getProperty( "mapred.jar", System.getProperty( AppProps.APP_JAR_PATH ) );

    if( property == null || property.isEmpty() )
      return null;

    return property;
    }
  }
