## What Is Ignite Thin Client

Visit [What Is Ignite Thin Client](https://apacheignite.readme.io/docs/thin-client) online.

## Quick Start

Visit [Quick Start](https://apacheignite.readme.io/docs/thin-client-quick-start) online.

## Design Considerations
The section explains design decisions and potential improvements.

### Ignite Core Dependency
The thin client component depends on ignite-core to re-use some public APIs and internal implementation. I think 
the right approach would be creating a separate ignite-api component that would:
* Expose public API.
* Provide Ignite Binary marshalling implementation.

Having such an ignite-api dependency would allow getting rid of the heavy "ignite-core" dependency while still 
enabling code-reuse.

Some entities like QueryEntity and CacheKeyConfiguration are re-defined inside the thin client component although they 
have ignite-core counterparts. The reasons are the Thin Client protocol defines different structure of those entities 
and the ignite-core entities include a lot of core-specific implementation not applicable to the thin client.

### API Design
The thin client component defines IgniteClient and CacheClient counterparts of the core Ignite and IgniteCache 
interfaces and **does not** implement them. The drawback of such a decision is that Ignite client node and thin client
cannot be used interchangeably. The reason for this decision was to avoid having lots of not implemented methods since
presently thin client provides a very small subset of the client node API. See 
[this discussion on Apache Ignite DEV lsit](http://apache-ignite-developers.2346864.n4.nabble.com/New-thin-client-and-interfaces-facades-td22023.html)
for more information.

JCache API (javax.cache.Cache) was not implemented for the same reason: there is still too much of unsupported JCache
functionality like Cache#invoke(). Eventually thin client will support JCache as the missing functionality is added.

### Async Methods are TODO
Presently all methods are synchronous and single-threaded. Thin client protocol allows de-coupling requests and 
responses to implement true async functionality. Adding async methods is one of the highest priority enhancements to
this component.

### Failover Limitations
Failover is fully implemented on the thin client side by restarting operation failed do to a node becoming unavailable.
That means the query API might return duplicate rows if a failover occurrred during iteration.

## Tests and Examples
At the moment only system tests have been developed. See 
[this discussion on Apache Ignite DEV list](http://apache-ignite-developers.2346864.n4.nabble.com/Adopting-mock-framework-ex-Mockito-for-unit-tests-tp26135.html)
to learn why system tests are given priority to unit tests within Ignite development community.

The system tests cover all implemented APIs and infrastructural concerns like failover and security. 

System tests do not include any mocking and thus can be used as examples to build real apps. 
 
Also, an example package org.apache.ignite.examples.thinclient including more samples was added to the ignite-examples 
module.
