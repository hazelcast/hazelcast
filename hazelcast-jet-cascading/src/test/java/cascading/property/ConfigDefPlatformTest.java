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

package cascading.property;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Insert;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static cascading.property.ConfigDef.Mode;
import static data.InputData.inputFileNums20;

/**
 *
 */
public class ConfigDefPlatformTest extends PlatformTestCase
  {
  public ConfigDefPlatformTest()
    {
    super( true );
    }

  public static class IterateInsert extends BaseOperation implements Function
    {
    private final boolean supportsNodeConfig;

    public IterateInsert( Fields fieldDeclaration, boolean supportsNodeConfig )
      {
      super( fieldDeclaration );

      this.supportsNodeConfig = supportsNodeConfig;
      }

    @Override
    public void prepare( FlowProcess flowProcess, OperationCall operationCall )
      {
      if( !( flowProcess instanceof FlowProcessWrapper ) )
        throw new RuntimeException( "not a flow process wrapper" );

      if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      if( !"pipe-replace".equals( flowProcess.getProperty( "replace" ) ) )
        throw new RuntimeException( "not replaced value" );

      if( supportsNodeConfig && !"node-replace".equals( flowProcess.getProperty( "default-node" ) ) )
        throw new RuntimeException( "not replaced value" );

      flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

      if( !"process-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      if( !"process-replace".equals( flowProcess.getProperty( "replace" ) ) )
        throw new RuntimeException( "not replaced value" );
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      functionCall.getOutputCollector().add( new Tuple( "value" ) );
      }

    @Override
    public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
      {
      }
    }

  @Test
  public void testPipeConfigDef() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new IterateInsert( new Fields( "value" ), getPlatform().isDAG() ), Fields.ALL );

    pipe.getConfigDef().setProperty( Mode.DEFAULT, "default", "pipe-default" );

    // steps on above value
    pipe.getStepConfigDef().setProperty( Mode.DEFAULT, "default", "process-default" );

    pipe.getConfigDef().setProperty( Mode.DEFAULT, "replace", "pipe-default" );
    pipe.getConfigDef().setProperty( Mode.REPLACE, "replace", "pipe-replace" );

    pipe.getNodeConfigDef().setProperty( Mode.REPLACE, "default-node", "node-replace" );

    pipe.getStepConfigDef().setProperty( Mode.DEFAULT, "replace", "process-default" );
    pipe.getStepConfigDef().setProperty( Mode.REPLACE, "replace", "process-replace" );

    pipe.getStepConfigDef().setProperty( Mode.DEFAULT, "default-node", "process-default" );

    Tap sink = getPlatform().getTextFile( getOutputPath( "configdef" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertTrue( flow.resourceExists( sink ) );
    }

  @Test
  public void testTapSourceConfigDef() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Scheme scheme = getPlatform().getTestConfigDefScheme();

    Tap source = getPlatform().getTap( scheme, inputFileNums20, SinkMode.KEEP );

    // process -> after sink/sourceConfInit are called
    // default -> Wrapper for all cluster side calls

    source.getConfigDef().setProperty( Mode.DEFAULT, "default", "source-default" );

    // steps on above value
    source.getStepConfigDef().setProperty( Mode.DEFAULT, "default", "process-default" );

    source.getConfigDef().setProperty( Mode.DEFAULT, "replace", "source-default" );
    source.getConfigDef().setProperty( Mode.REPLACE, "replace", "source-replace" );

    source.getNodeConfigDef().setProperty( Mode.REPLACE, "default-node", "node-replace" );

    source.getStepConfigDef().setProperty( Mode.DEFAULT, "replace", "process-default" );
    source.getStepConfigDef().setProperty( Mode.REPLACE, "replace", "process-replace" );

    source.getStepConfigDef().setProperty( Mode.DEFAULT, "default-node", "process-default" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Insert( new Fields( "value" ), "nada" ), Fields.ALL );

    Tap sink = getPlatform().getTextFile( getOutputPath( "tapsourceconfigdef" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertTrue( flow.resourceExists( sink ) );
    }

  @Test
  public void testTapSinkConfigDef() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20, SinkMode.KEEP );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Insert( new Fields( "value" ), "nada" ), Fields.ALL );

    Scheme scheme = getPlatform().getTestConfigDefScheme();

    Tap sink = getPlatform().getTap( scheme, getOutputPath( "tapsinkconfigdef" ), SinkMode.REPLACE );

    // process -> after sink/sourceConfInit are called
    // default -> Wrapper for all cluster side calls
    sink.getConfigDef().setProperty( Mode.DEFAULT, "default", "sink-default" );

    // steps on above value
    sink.getStepConfigDef().setProperty( Mode.DEFAULT, "default", "process-default" );

    sink.getConfigDef().setProperty( Mode.DEFAULT, "replace", "sink-default" );
    sink.getConfigDef().setProperty( Mode.REPLACE, "replace", "sink-replace" );

    sink.getNodeConfigDef().setProperty( Mode.REPLACE, "default-node", "node-replace" );

    sink.getStepConfigDef().setProperty( Mode.DEFAULT, "replace", "process-default" );
    sink.getStepConfigDef().setProperty( Mode.REPLACE, "replace", "process-replace" );

    sink.getStepConfigDef().setProperty( Mode.DEFAULT, "default-node", "process-default" );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertTrue( flow.resourceExists( sink ) );
    }

  public static class ConfigSubAssembly extends SubAssembly
    {
    public ConfigSubAssembly( Pipe pipe, boolean supportsNodeConfig )
      {
      super( pipe );

      pipe = new Each( pipe, new IterateInsert( new Fields( "value" ), supportsNodeConfig ), Fields.ALL );

      setTails( pipe );
      }
    }

  @Test
  public void testSubAssemblyConfigDef() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20 );

    Pipe pipe = new Pipe( "test" );

    pipe = new ConfigSubAssembly( pipe, getPlatform().isDAG() );

    pipe.getConfigDef().setProperty( Mode.DEFAULT, "default", "pipe-default" );

    // steps on above value
    pipe.getStepConfigDef().setProperty( Mode.DEFAULT, "default", "process-default" );

    pipe.getConfigDef().setProperty( Mode.DEFAULT, "replace", "pipe-default" );
    pipe.getConfigDef().setProperty( Mode.REPLACE, "replace", "pipe-replace" );

    pipe.getNodeConfigDef().setProperty( Mode.REPLACE, "default-node", "node-replace" );

    pipe.getStepConfigDef().setProperty( Mode.DEFAULT, "replace", "process-default" );
    pipe.getStepConfigDef().setProperty( Mode.REPLACE, "replace", "process-replace" );

    pipe.getStepConfigDef().setProperty( Mode.DEFAULT, "default-node", "process-default" );

    Tap sink = getPlatform().getTextFile( getOutputPath( "subassembly-configdef" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertTrue( flow.resourceExists( sink ) );
    }
  }
