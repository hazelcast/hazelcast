

# User Defined Services

In the case of special/custom needs, Hazelcast's SPI (Service Provider Interface) module offers the users to develop their own distributed data structures and services.


## Sample Case

Throughout this section, a distributed counter that we are going to create, will be the guide to reveal the usage of Hazelcast SPI.

Here is our counter.

```java
public interface Counter{
   int inc(int amount);
}
```

This counter will have the below features:
- It is planned to be stored in Hazelcast. 
- Different cluster members can call it. 
- It will be scalable, meaning that the capacity for the number of counters scales with the number of cluster members.
- It will be highly available, meaning that if a member hosting this counter goes down, a backup will be available on a different member.

All these features are going to be realized with the steps below. In each step, a new functionality to this counter will be added.

1. Create the class.
2. Enable the class.
3. Add properties.
5. Place a remote call.
5. Create the containers.
6. Enable partition migration.
6. Create the backups.



