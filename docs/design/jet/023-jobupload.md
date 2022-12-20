# Jet job submission from non-java client

### Table of Contents

+ [Background](#background)
  - [Description](#description)
  - [Terminology](#terminology)
  - [Actors and Scenarios](#actors-and-scenarios)
+ [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
  * [Additional Functional Design Topics](#additional-functional-design-topics)
    + [Notes/Questions/Issues](#notesquestionsissues)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Client Related Changes](#client-related-changes)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)
+ [Other Artifacts](#other-artifacts)


|                                |                                                  |
|--------------------------------|--------------------------------------------------|
| Related Jira                   | _https://hazelcast.atlassian.net/browse/HZ-1455_ |
| Related Github issues          | _None_                                           |
| Document Status / Completeness | DRAFT                                            |
| Requirement owner              | _TBD_                                            |
| Developer(s)                   | _Orçun Çolak_                                    |
| Quality Engineer               | _TBD_                                            |
| Support Engineer               | _TBD_                                            |
| Technical Reviewers            | _Frantisek Hartman_                              |
| Simulator or Soak Test PR(s)   | _TBD_                                            |

### Background
#### Description

- Currently the job upload can only be perfomed by hz-cli command. This command required a JVM on the host machine. The purpose of this feature is to enable non-java clients to be able to upload jobs to Jet server 


#### Terminology

| Term                   | Definition                                                              |
|------------------------|-------------------------------------------------------------------------|
| Client Binary Protocol | The binary messages used between Hazelcast Clients and Hazelcast Server |

#### Actors and Scenarios

Any language that implements Client Binary Protocol

### Functional Design
#### Summary of Functionality

Provide a list of functions user(s) can perform.
- Upload a jar file and pass the same parameters that can be used with hz-cli

#### Additional Functional Design Topics

None

##### Notes/Questions/Issues

- The new functionality requires that all resources accessed by the uploaded job to be available on the server. For example a text file that is used to populate an IMap needs to be available on the server or within the uploaded jar. Because a server side job can not access client side resources.  
- The client should send multi parts in a sequential manner. Out of order messages are not handled since it would require more resources on the server.
  

### User Interaction
#### API design and/or Prototypes

```java
HazelcastInstance client = ...;
JetService jetService = client.getJet();
List<String> jobParameters = emptyList();

String job1 = "job1";
//If there is an error, throws JetException
jetService.uploadJob(getJarPath(),
        null,
        job1,
        null,
        jobParameters);
```

#### Client Related Changes
A new method has been added to JetService interface. Any client that implements this interface, has to implement this method
```java
void uploadJob(@Nonnull Path jarPath, String snapshotName, String jobName, String mainClass, List<String> jobParameters);
```

### Technical Design

The client protocol needs to support job uploading. So that non-java clients can also upload jet jobs.
For this purpose two new messages are added to client protocol

1. uploadJobMetaData
2. uploadJobMultipart

**1. uploadJobMetaData Message**

The upload process starts with uploadJobMetaData. This message contains

| Term                   | Type | Definition                                                       |
|------------------------|------|------------------------------------------------------------------|
| sessionId | UUID | The UUID. This field associates all messages in a session |
| jarSize |    long  | The jar size in bytes                                            |
| snapshotName |  String    | Argument passed when starting the job                            |
| jobName |    String  | Argument passed when starting the job                            |
| mainClass |   String   | Argument passed when starting the job                            |
| jobParameters |   List_String   | Argument passed when starting the job                            |


Upon reception of uploadJobMetaData message, the server performs some validation rules. If any of them fail, a **JetException** is thrown. If all is well, server stores a new entry in the JobUploadStore class

**2. uploadJobMultipart message**

The upload process continues with this message. It contains jar's bytes. This message contains these fields

| Term                   | Type | Definition                                                              |
|------------------------|------|-------------------------------------------------------------------------|
| sessionId | UUID | It is explained in the previous message |
| currentPartNumber |   int   | It starts from 1 and shows the sequence number of the part. For example 1 of 5 |
| totalPartNumber |    int  | It is the total number of parts of the sequence  |
| partData |    byteArray  | is the **byte[]** containing jar data  |
| partSize |  int    | shows how many bytes of the partData byte[] is valid, |

**Why do we need an extra partSize field?** 

For optimization, it is assumed that partData is allocated only once on the client side. So we need another field to indicate the number of bytes to be read in this buffer 

Upon reception of uploadJobMultipart, various checks are performed on the message and current session. If any of them fail, a **JetException** is thrown. If all is well, message is processed.
If it is the first message a new temporary file is created. Then the partSize bytes of partData byte[] is appended to this file
When all the parts are complete, a new job is started using HazelcastBootstrap.executeJar() call. When the job starts, the jar is loaded into memory and it is not possible to delete it anymore, because it is in use.

The uploadJobMultipart by default allocates a buffer of 10_000_000 bytes. The size of the buffer can be controlled by
**hazelcast.jobupload.partsize** system property. So clients that want to allocate less memory may prefer to send a bigger total number of messages

HazelcastBootstrap was designed to work only by the **hz-client command**. With this PR it is modified to work on the server side. However, it is still a singleton.

If any exception is thrown by the server or client fails to finish the upload sequence, a timer in JetServiceBackend cleans the expired JobUploadStore items and deletes the temporary file

### Testing Criteria

Describe testing approach to developed functionality
- Stress tests need to be performed with big jars or small upload buffer size


### Other Artifacts

None
