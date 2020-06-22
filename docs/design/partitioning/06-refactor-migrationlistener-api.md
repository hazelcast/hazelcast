# Refactor MigrationListener API

|ℹ️ Since: 4.0| 
|-------------|  


## Background

### Description

Current `MigrationListener` API is not very handy. On each migration a
pair of *migration started* and *migration completed or failed* events
are fired. But having separate events for migration start and complete
doesn't add any value. We can only notify when a replica migration is
completed (either success or failure). Additionally, migration events
are fired only for primary replica migrations but we migrate/replicate
backup replicas using the same mechanism.

Also another thing missing is, there's no event for start and completion
of whole migration process. Generally users are interested in whether
there's an ongoing migration process or not.

### Terminology

| Term                          | Definition                                                        |
|-------------------------------|-------------------------------------------------------------------|
| Migration Process             | A group of partition migrations planned together in a single shot |
| Partition (Replica) Migration | Migration of a single replica of a single partition               |

  

## Functional Design

### Summary of Functionality

Migration listener will notify users when the migration process starts,
after each partition replica migration is completed and finally when
migration process ends. On each step, users can see how many migrations
are planned, how many completed, how much time elapsed etc.

## User Interaction

### API design and/or Prototypes

  
##### **MigrationListener**

```java
public interface MigrationListener extends EventListener {
    /**
     * Called when the migration process starts.
     * A migration process consists of a group of partition
     * replica migrations which are planned together.
     * <p>
     * When migration process is completed, {@link #migrationFinished(MigrationState)}
     * is called.
     */
    void migrationStarted(MigrationState state);

    /**
     * Called when the migration process finishes.
     * This event denotes ending of migration process which is
     * started by {@link #migrationStarted(MigrationState)}.
     * <p>
     * Not all of the planned migrations have to be completed.
     * Some of them can be skipped because of a newly created migration plan.
     * <p>
     * If migration process coordinator member (generally the oldest member in cluster)
     * crashes before migration process ends, then this method may not be called at all.
     */
    void migrationFinished(MigrationState state);

    /**
     * Called when a partition replica migration is completed successfully.
     */
    void replicaMigrationCompleted(ReplicaMigrationEvent event);

    /**
     * Called when a partition replica migration is failed.
     */
    void replicaMigrationFailed(ReplicaMigrationEvent event);
}
```

  

##### **ReplicaMigrationEvent**

```java
public interface ReplicaMigrationEvent extends PartitionEvent {
    /**
     * Returns the index of the partition replica.
     * 0th index is primary replica, 1st index is 1st backup and so on.
     */
    int getReplicaIndex();

    /**
     * Returns the old owner of the migrating partition replica.
     */
    Member getSource();

    /**
     * Returns the new owner of the migrating partition replica.
     */
    Member getDestination();

    /**
     * Returns the result of the migration: completed or failed.
     */
    boolean isSuccess();

    /**
     * Returns the elapsed the time of this migration in milliseconds.
     *
     * @return elapsed time in milliseconds.
     */
    long getElapsedTime();

    /**
     * Returns the progress information of the overall migration.
     */
    MigrationState getMigrationState();
}
```

  

##### **MigrationState**

```java
public interface MigrationState {
    /**
     * Returns the start time of the migration in milliseconds since the epoch.
     */
    long getStartTime();

    /**
     * Returns the number of planned migrations in the migration plan.
     */
    int getPlannedMigrations();

    /**
     * Returns the number of completed migrations in the migration plan.
     */
    int getCompletedMigrations();

    /**
     * Returns the number of remaining migrations in the migration plan.
     */
    int getRemainingMigrations();

    /**
     * Returns the total elapsed time of completed migrations in milliseconds.
     */
    long getTotalElapsedTime();
}
```

  

## Technical Design

In former API, there was no notion of migration process. There were only
single migration start/complete events. That's why it was impossible to
know when the first migration is started, how many migrations are
planned, how many remaining etc. New API introduces notion of *migration
process*, which is group of partition migrations planned together. It
starts with a new *migration plan*, shows the progress after each
completed migration and shows the final migration process status when
process ends. Process may end before completing all planned migrations.
This happens when cluster membership state changes (a new member joins
or an existing one leaves) and a new migration process is started with a
new plan.

Migration manager implementation already tracks statistics about
migration start time, number of planned migrations, number of completed
migrations and elapsed times of migrations. Even it tracks elapsed times
of some internal steps of the migrations. With the new migration
listener API, we will expose these stats to the user with the migration
progress events.

A new migration process is started for a few cases:

-   *When a partition rebalance is required after a new member joins or
    an existing member leaves:*  
    Migration tasks for rebalance and for creating missing replicas will
    form a single migration process.

-   *When backup to primary promotion is required after an existing
    member terminates/crashes:*  
    Promotion tasks on each member will form a single migration process.
    Promotions are committed to each member serially, one-by-one. So
    during promotion commits, migration processes for each member will
    start and finish in sequence.

-   *When some partitions lost completely after a member crash when
    partition table is frozen:*  
    Migrations tasks to assign new owners for lost partition replicas
    will form a single migration process.

Note that, migration process completion event may not be published when
master (migration coordinator) crashes while migration process in
ongoing. Since other members do not know the whole migration plan, they
cannot continue executing it. New master will start a new migration
process with a new plan but former migration process will not receive a
completion event.

Another important change is, old listener API was publishing migration
events only for primary (0th) replica migrations. With the new one, we
will publish events for all replica migrations, both for primary and
backup migrations.

## Testing Criteria

Updated existing unit tests to conform new listener API, also added new
unit tests for the new API methods. 
