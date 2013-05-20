### riaktor
Riak data access using [Reactor](https://github.com/reactor/reactor/).

---

Riaktor is a wrapper around the official Riak Java client that provides efficient asynchronous task execution and coordination using [Reactor](https://github.com/reactor/reactor/).

It doesn't implement low-level networking. It just provides a way to execute submitted `RiakOperation<?>` instances in Reactor's efficient task dispatching mechanism. You work with `Riaktor` as you would in a "normal", blocking Java application, but rather than calling `execute()` and blocking your calling thread, you submit the operation to Riaktor to be executed in a `Dispatcher`.

### Building and installing

To use this early, experimental code, you'll need to install the Maven artifacts locally:

    git clone git://github.com/jbrisbin/riaktor.git
    cd riaktor
    ./gradlew install

The tests assume you have a Riak instance running locally.

### Promises

Most methods on the `Riaktor` object return a `Promise<T>`. Riaktor uses promises to easily coordinate dependent tasks. You can either block the calling thread to get the value of a promise (that's the easiest way to do testing), you can set callbacks, or you can use the composition methods of Reactor's `Composable` abstraction.

#### map, filter, reduce

Since Riaktor returns a `Promise`, you can use the efficient `map`, `filter`, and `reduce` methods to interact with the returned values. Riaktor does not yet support dealing with `Composable` in iterating over query results.

### Groovy Example

This example fetches a bucket and asynchronously executes a `map` function that passes the `Bucket` and expects a return value, which in this case is also a `Promise` as returned from `Riaktor.store`:

		def p = riaktor.fetchBucket("test").
      map({ Bucket b ->
        riaktor.store(b, "test", data)
      }).await(5, SECONDS)

You can find out when the store operation is complete by consuming on the return of `map`, which is actually the return of `riaktor.store`:

    p.consume {
      // store will return 'null' since I'm not specifying to return the body
      counter.decrementAndGet()
    }

### Use all the CAP tools you want

Nothing is hidden in `Riaktor`. There are several overrides of all the methods that allow you pass conflict resolvers, mutations and the like. You can also send a single operation (or, more efficiently, a batch of them) to Riak. If you want to use the `Bucket` object to build up an update with the appropriate W and DW settings, then you create a `StoreObject` like you normally would. It's just that instead of calling `execute()`, you would call `Riaktor.send` and schedule that operation to be run in a Dispatcher, dealing with the result that will eventually be returned in a `Promise`.

---

Riaktor is Apache 2.0 licensed.