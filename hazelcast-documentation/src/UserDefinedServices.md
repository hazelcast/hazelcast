

## User Defined Services

In the case of special/custom needs, Hazelcast's SPI (Service Provider Interface) module offers the users to develop their own distributed data structures and services.

### What Can Be Accomplished

List the most famous things that a user can do using SPI:

- ???
- ???
- ???


### Creating a Custom DDS
???Steps for creating a custom distributed data structure???



### Creating a Custom Service
???Steps for creating a custom service???

### Sample Case

Throughout this section, a distributed counter will be the guide to reveal Hazelcast SPI.

Let's start.

```java
public interface Counter{
   int inc(int amount);
}
```

`Counter` will be stored in Hazelcast and it can be called by each cluster member. It will also be scalable; so the capacity for the number of counters scales with the number of members in the cluster. And it will be highly available, so if a member hosting that counter fails, a backup will already be available on a different member and the system will continue as if nothing happened. We are going to do this step by step; in each section a new piece of functionality is going to be added.

???
