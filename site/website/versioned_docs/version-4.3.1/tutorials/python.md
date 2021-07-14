---
title: Apply a Python Function
description: How to run Python code to process data in Jet pipelines.
id: version-4.3.1-python
original_id: python
---

Hazelcast Jet allows you to write a function in Python and use it to
transform the data flowing through a data pipeline. You are expected to
write a function that takes a list of strings and returns the
transformed list of strings.

## 1. Write a Python Function

Here is the function we want to apply:

```python
import numpy as np

def transform_list(input_list):
    num_list = [float(it) for it in input_list]
    sqrt_list = np.sqrt(num_list)
    return ["sqrt(%d) = %.2f" % (x, y) for (x, y) in zip(num_list, sqrt_list)]
```

Save this code to `take_sqrt.py` in a folder of your choosing, we'll
call it `<python_src>`. Since our code uses `numpy`, we need a
requirements file that names it:

```text
numpy
```

Save this as `requirements.txt` in the `<python_src>` folder.

## 2. Install Python 3

Hazelcast Jet works with Python 3.5-3.7. Check whether you have it:

```bash
$ python3 --version
Python 3.7.5
```

If the version is not right or `python3` is not recognized, install
Python 3 on your machine.

## 3. Start Hazelcast Jet

1. Download Hazelcast Jet

```shell
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.3.1/hazelcast-jet-4.3.1.tar.gz
tar zxvf hazelcast-jet-4.3.1.tar.gz && cd hazelcast-jet-4.3.1
```

If you already have Jet and you skipped the above steps, make sure to
follow from here on.

2. Activate the Python plugin:

```bash
mv opt/hazelcast-jet-python-4.3.1.jar lib/
```

3. Start Jet:

```bash
bin/jet-start
```

4. When you see output like this, Hazelcast Jet is up:

```text
Members {size:1, ver:1} [
    Member [192.168.1.5]:5701 - e7c26f7c-df9e-4994-a41d-203a1c63480e this
]
```

From now on we assume Hazelcast Jet is running on your machine.

## 4. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`tutorial-python` and copy the Gradle or Maven file into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:4.3.1'
    compile 'com.hazelcast.jet:hazelcast-jet-python:4.3.1'
}

jar.manifest.attributes 'Main-Class': 'org.example.JetJob'
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>tutorial-python</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet</artifactId>
            <version>4.3.1</version>
        </dependency>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet-python</artifactId>
            <version>4.3.1</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>org.example.JetJob</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 5. Apply the Python Function to a Jet Pipeline

This code generates a stream of numbers and lets Python take their
square roots. Make sure to set the right path in the `.setBaseDir` line:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.python.PythonServiceConfig;

import static com.hazelcast.jet.python.PythonTransforms.mapUsingPython;

public class JetJob {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(10, (ts, seq) -> String.valueOf(seq)))
         .withoutTimestamps()
         .apply(mapUsingPython(new PythonServiceConfig()
                 .setBaseDir("<python_src>")
                 .setHandlerModule("take_sqrt")))
         .setLocalParallelism(1)
         .writeTo(Sinks.logger());

        JobConfig cfg = new JobConfig().setName("python-function");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }
}
```

You may run this code from your IDE and it will work, but it will create
its own Jet instance. To run it on the Jet instance you already started,
use the command line like this:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
<path_to_jet>/bin/jet submit build/libs/tutorial-python-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
mvn package
<path_to_jet>/bin/jet submit target/tutorial-python-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

Now go to the window where you started Jet. Its log output will contain
the output from the pipeline, like this:

```text
15:41:58.411 [ INFO] ... sqrt(0) = 0.00
15:41:58.411 [ INFO] ... sqrt(1) = 1.00
15:41:58.411 [ INFO] ... sqrt(2) = 1.41
15:41:58.411 [ INFO] ... sqrt(3) = 1.73
15:41:58.411 [ INFO] ... sqrt(4) = 2.00
15:41:58.412 [ INFO] ... sqrt(5) = 2.24
15:41:58.412 [ INFO] ... sqrt(6) = 2.45
15:41:58.412 [ INFO] ... sqrt(7) = 2.65
```

Once you're done with it, cancel the job:

```bash
<path_to_jet>/bin/jet cancel python-function
```
