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

package cascading.detail;

import java.util.Map;
import java.util.Properties;

import cascading.pipe.Pipe;
import cascading.platform.TestPlatform;
import junit.framework.Test;
import junit.framework.TestSuite;

public class EveryEveryPipeAssemblyPlatformTest extends PipeAssemblyTestBase
  {
  public static Test suite( TestPlatform testPlatform ) throws Exception
    {
    TestSuite suite = new TestSuite();

    Properties properties = loadProperties( "op.op.properties" );

    String runOnly = runOnly( properties );

    Map<String, Pipe> pipes = buildOpPipes( null, new Pipe( "every.every" ), new EachAssemblyFactory(), LHS_ARGS_FIELDS, LHS_DECL_FIELDS, LHS_SELECT_FIELDS, LHS_VALUE, null );
    for( String name : pipes.keySet() )
      makeSuites( testPlatform, properties, buildOpPipes( name, pipes.get( name ), new EachAssemblyFactory(), RHS_ARGS_FIELDS, RHS_DECL_FIELDS, RHS_SELECT_FIELDS, RHS_VALUE, runOnly ), suite, EveryEveryPipeAssemblyPlatformTest.class );

    return suite;
    }

  public EveryEveryPipeAssemblyPlatformTest( Properties properties, String displayName, String name, Pipe pipe )
    {
    super( properties, displayName, name, pipe );
    }
  }