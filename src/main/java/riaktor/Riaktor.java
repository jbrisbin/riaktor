package riaktor;

import com.basho.riak.client.*;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.operations.StoreObject;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.core.CachingRegistry;
import reactor.core.Promise;
import reactor.core.R;
import reactor.core.Reactor;
import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.RingBufferDispatcher;
import reactor.fn.selector.BaseSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static reactor.core.Context.rootDispatcher;

/**
 * Instances of this class manage the execution of {@link RiakOperation RiakOperations} so that the user doesn't
 * directly call {@link com.basho.riak.client.operations.RiakOperation#execute()}. All operations are asynchronous and
 * the Reactor is used to ensure that operational load for the IO is distributed according to the {@link Dispatcher}
 * being provided in the {@literal ioDispatcher} constructor argument.
 * <p/>
 * By default, {@literal Riaktor} uses {@link RingBufferDispatcher RingBufferDispatchers} for both event handling and IO
 * because it's the fastest.
 *
 * @author Jon Brisbin
 */
public class Riaktor extends Reactor {

	private final Logger           log            = LoggerFactory.getLogger(Riaktor.class);
	private final Registry<Bucket> bucketRegistry = new CachingRegistry<>(null, null);
	private final IRiakClient riakClient;
	private final Reactor     ioReactor;

	/**
	 * Create a {@literal Riaktor} using the default protobuf client.
	 *
	 * @throws RiakException
	 * @see {@link com.basho.riak.client.RiakFactory#pbcClient()}
	 */
	public Riaktor() throws RiakException {
		this(RiakFactory.pbcClient());
	}

	/**
	 * Create a {@literal Riaktor} using the given client.
	 *
	 * @param riakClient The {@link IRiakClient} to use.
	 */
	public Riaktor(IRiakClient riakClient) {
		this(riakClient, rootDispatcher());
	}

	public Riaktor(@Nonnull IRiakClient riakClient, @Nonnull Dispatcher customDispatcher) {
		this(riakClient, customDispatcher, new RingBufferDispatcher(
				"riaktor",
				1,
				1024,
				ProducerType.MULTI,
				new YieldingWaitStrategy()
		));
	}

	/**
	 * Create a {@literal Riaktor} using the given client, {@link Dispatcher} and IO {@link Dispatcher}.
	 *
	 * @param riakClient       The {@link IRiakClient} to use.
	 * @param customDispatcher The {@link Dispatcher} that event {@link Consumer Consumers} will use.
	 * @param ioDispatcher     The {@link Dispatcher} the {@link Reactor} responsible for IO will use.
	 */
	public Riaktor(@Nonnull IRiakClient riakClient, @Nonnull Dispatcher customDispatcher, @Nonnull Dispatcher ioDispatcher) {
		super(customDispatcher);
		this.riakClient = riakClient;
		this.ioReactor = new Reactor(ioDispatcher);
	}

	/**
	 * Send a batch of operations to Riak and schedule them all to be executed by queueing them in the IO Reactor.
	 *
	 * @param ops The bathc of operations to submit.
	 * @return A {@link Promise} that will be completed after all operations have been executed on the server.
	 */
	public Promise<Void> send(RiakOperation<?>... ops) {
		final Promise<Void> p = new Promise<>(this);
		final AtomicLong counter = new AtomicLong(ops.length);

		for (final RiakOperation<?> op : ops) {
			R.schedule(
					new Consumer<Void>() {
						@Override
						public void accept(Void v) {
							try {
								op.execute();
							} catch (RiakException e) {
								Riaktor.this.notify(Fn.T(e.getClass()), Fn.event(e));
							} finally {
								if (counter.decrementAndGet() == 0) {
									p.set((Void) null);
								}
							}
						}
					},
					null,
					ioReactor
			);
		}

		return p;
	}

	/**
	 * Schedule a single operation to be performed on the server, returning a {@link Promise} that will be completed when
	 * the operation is executed and a result is optionally available.
	 *
	 * @param op  The operation to execute on the server.
	 * @param <T> The type of the operation return value.
	 * @param <O> The type of the operation.
	 * @return A {@link Promise} representing the optional return value from {@link com.basho.riak.client.operations.RiakOperation#execute()}.
	 */
	public <T, O extends RiakOperation<T>> Promise<T> send(@Nonnull final O op) {
		final Promise<T> p = new Promise<>(this);

		R.schedule(
				new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						try {
							T result = op.execute();
							if (log.isTraceEnabled()) {
								log.trace("{} result: {}", op, result);
							}
							p.set(result);
						} catch (RiakException e) {
							p.set(e);
						}
					}
				},
				null,
				ioReactor
		);

		return p;
	}

	/**
	 * Fetch a {@link Bucket} with the given name.
	 *
	 * @param name The name of the bucket to fetch.
	 * @return A {@link Promise} that will be completed when the {@link Bucket} is available.
	 */
	public Promise<Bucket> fetchBucket(@Nonnull final String name) {
		return fetchBucket(name, false);
	}

	/**
	 * Fetch a {@link Bucket} with the given name and optionally ignore any cached buckets that have been previously
	 * fetched.
	 *
	 * @param name    The name of the bucket to fetch.
	 * @param refresh {@literal true} to ignore the cache, {@literal false} otherwise.
	 * @return A {@link Promise} that will be completed when the {@link Bucket} is available.
	 */
	public Promise<Bucket> fetchBucket(@Nonnull final String name, final boolean refresh) {
		final Promise<Bucket> p = new Promise<>(this);

		Iterator<Registration<? extends Bucket>> buckets = bucketRegistry.select(name).iterator();
		if (refresh || !buckets.hasNext()) {
			R.schedule(
					new Consumer<Void>() {
						@Override
						public void accept(Void v) {
							try {
								Bucket b = riakClient.fetchBucket(name).execute();
								if (refresh) {
									bucketRegistry.unregister(b);
								}
								bucketRegistry.register(new BucketSelector(b), b);
								if (log.isTraceEnabled()) {
									log.trace("Fetched: {}", b);
								}
								p.set(b);
							} catch (Throwable t) {
								p.set(t);
							}
						}
					},
					null,
					ioReactor
			);
		} else {
			p.set(buckets.next().getObject());
		}

		return p;
	}

	/**
	 * Store a value in Riak using the given bucket, key, and value.
	 *
	 * @param bucket The {@link Bucket} in which to store the data.
	 * @param key    The key to use.
	 * @param value  The value to be stored.
	 * @param <T>    The type of the value to be stored.
	 * @return A {@link Promise} that will be completed when the store operation has completed.
	 */
	public <T> Promise<T> store(@Nonnull final Bucket bucket,
															@Nullable final String key,
															@Nullable final T value) {
		return store(bucket, key, value, null, null, null);
	}

	/**
	 * Store a value in Riak using the given bucket, key, and value. Use the given {@link Function} to resolve any
	 * conflicts.
	 *
	 * @param bucket           The {@link Bucket} in which to store the data.
	 * @param key              The key to use.
	 * @param value            The value to be stored.
	 * @param conflictResolver (optional) The {@link Function} to use to resolve any conflicts. May be null.
	 * @param <T>              The type of the value to be stored.
	 * @return A {@link Promise} that will be completed when the store operation has completed.
	 */
	public <T> Promise<T> store(@Nonnull final Bucket bucket,
															@Nullable final String key,
															@Nullable final T value,
															@Nullable final Function<Collection<T>, T> conflictResolver) {
		return store(bucket, key, value, conflictResolver, null, null);
	}

	/**
	 * Store a value in Riak using the given bucket, key, and value. Use the given {@link Function} to resolve any
	 * conflicts, convert as necessary using the given {@link Converter}, and mutate the value using the given {@link
	 * Mutation}.
	 *
	 * @param bucket           The {@link Bucket} in which to store the data.
	 * @param key              The key to use.
	 * @param value            The value to be stored.
	 * @param conflictResolver The {@link Function} to use to resolve any conflicts.
	 * @param converter        The {@link Converter} to use.
	 * @param mutation         The {@link Mutation} to use.
	 * @param <T>              The type of the value.
	 * @return A {@link Promise} that will be completed when the store operation has completed.
	 */
	@SuppressWarnings({"unchecked"})
	public <T> Promise<T> store(@Nonnull final Bucket bucket,
															@Nullable final String key,
															@Nullable final T value,
															@Nullable final Function<Collection<T>, T> conflictResolver,
															@Nullable final Converter<T> converter,
															@Nullable final Mutation<T> mutation) {
		final Promise<T> p = new Promise<>(this);

		R.schedule(
				new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						boolean valIsString = String.class.isInstance(value);
						boolean valIsBytes = byte[].class.isInstance(value);
						StoreObject<T> op;
						if (null == key && valIsString) {
							op = (StoreObject<T>) bucket.store((String) value);
						} else if (null == key && valIsBytes) {
							op = (StoreObject<T>) bucket.store((byte[]) value);
						} else if (null == key) {
							op = bucket.store(value);
						} else if (valIsString) {
							op = (StoreObject<T>) bucket.store(key, (String) value);
						} else if (valIsBytes) {
							op = (StoreObject<T>) bucket.store(key, (byte[]) value);
						} else {
							op = bucket.store(key, value);
						}
						if (log.isTraceEnabled()) {
							log.trace("Preparing operation {}", op);
						}
						if (null != conflictResolver) {
							op = op.withResolver(
									new ConflictResolver<T>() {
										@Override
										public T resolve(Collection<T> siblings) {
											T result = conflictResolver.apply(siblings);
											Riaktor.this.notify("/" + bucket.getName() + "/" + key, new MergeEvent<T>(Tuple.of(siblings, result)));
											return result;
										}
									}
							);
						}
						if (null != converter) {
							op = op.withConverter(converter);
						}
						if (null != mutation) {
							op = op.withMutator(mutation);
						}

						try {
							T result = op.execute();
							if (log.isTraceEnabled()) {
								log.trace("/{}/{} stored: {}", bucket.getName(), key, result);
							}
							Riaktor.this.notify("/" + bucket.getName() + "/" + key, new StoreEvent<>(result));

							p.set(result);
						} catch (RiakRetryFailedException e) {
							p.set(e);
						}
					}
				},
				null,
				ioReactor
		);

		return p;
	}

	/**
	 * Fetch a value from Riak using the given bucket, key, and returning as an {@link IRiakObject}.
	 *
	 * @param bucket The bucket from which to fetch the data.
	 * @param key    The key to use.
	 * @return A {@link Promise} that will be completed when the value has been fetched.
	 */
	public Promise<IRiakObject> fetch(@Nonnull final Bucket bucket,
																		@Nonnull final String key) {
		return fetch(bucket, key, null, null, null);
	}

	/**
	 * Fetch a value from Riak using the given bucket, key, and returning as the given type.
	 *
	 * @param bucket The bucket from which to fetch the data.
	 * @param key    The key to use.
	 * @param asType The type to return the value as.
	 * @param <T>    The type of the return value.
	 * @return A {@link Promise} that will be completed when the value has been fetched.
	 */
	public <T> Promise<T> fetch(@Nonnull final Bucket bucket,
															@Nonnull final String key,
															@Nullable final Class<T> asType) {
		return fetch(bucket, key, asType, null, null);
	}

	/**
	 * Fetch a value from Riak using the given bucket, key, and returning as the given type. Use the given {@link Function}
	 * to resolve any conflicts.
	 *
	 * @param bucket           The bucket from which to fetch the data.
	 * @param key              The key to use.
	 * @param asType           The type to return the value as.
	 * @param conflictResolver The {@link Function} to use to resolve any conflicts.
	 * @param <T>              The type of the return value.
	 * @return A {@link Promise} that will be completed when the value has been fetched.
	 */
	public <T> Promise<T> fetch(@Nonnull final Bucket bucket,
															@Nonnull final String key,
															@Nullable final Class<T> asType,
															@Nullable final Function<Collection<T>, T> conflictResolver) {
		return fetch(bucket, key, asType, conflictResolver, null);
	}

	/**
	 * Fetch a value from Riak using the given bucket, key, and returning as the given type. Use the given {@link Function}
	 * to resolve any conflicts and convert as necessary using the given {@link Converter}.
	 *
	 * @param bucket           The bucket from which to fetch the data.
	 * @param key              The key to use.
	 * @param asType           The type to return the value as.
	 * @param conflictResolver The {@link Function} to use to resolve any conflicts.
	 * @param converter        The {@link Converter} to use.
	 * @param <T>              The type of the return value.
	 * @return A {@link Promise} that will be completed when the value has been fetched.
	 */
	@SuppressWarnings({"unchecked"})
	public <T> Promise<T> fetch(@Nonnull final Bucket bucket,
															@Nonnull final String key,
															@Nullable final Class<T> asType,
															@Nullable final Function<Collection<T>, T> conflictResolver,
															@Nullable final Converter<T> converter) {
		final Promise<T> p = new Promise<>(this);

		R.schedule(
				new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						FetchObject<T> op;
						if (null == asType || String.class.equals(asType) || byte[].class.equals(asType)) {
							op = (FetchObject<T>) bucket.fetch(key);
						} else {
							op = bucket.fetch(key, asType);
						}
						if (null != conflictResolver) {
							op = op.withResolver(
									new ConflictResolver<T>() {
										@Override
										public T resolve(Collection<T> siblings) {
											T result = conflictResolver.apply(siblings);
											Riaktor.this.notify("/" + bucket.getName() + "/" + key, new MergeEvent<T>(Tuple.of(siblings, result)));
											return result;
										}
									}
							);
						}
						if (null != converter) {
							op = op.withConverter(converter);
						}

						try {
							T result = op.execute();
							if (log.isTraceEnabled()) {
								log.trace("/{}/{} fetched: {}", bucket.getName(), key, result);
							}
							if (String.class == asType && IRiakObject.class.isInstance(result)) {
								p.set((T) ((IRiakObject) result).getValueAsString());
							} else if (String.class == asType && IRiakObject.class.isInstance(result)) {
								p.set((T) ((IRiakObject) result).getValue());
							} else {
								p.set(result);
							}
						} catch (RiakRetryFailedException e) {
							p.set(e);
						}
					}
				},
				null,
				ioReactor
		);
		return p;
	}

	/**
	 * Delete a value using the given bucket and key.
	 *
	 * @param bucket The bucket to use.
	 * @param key    The key to use.
	 * @return A {@link Promise} that will be completed when the value has been deleted.
	 */
	public Promise<Void> delete(Bucket bucket, String key) {
		return delete(bucket, key, null);
	}

	/**
	 * Delete a value using the given bucket and key. Retry the operation using the given {@link Retrier}.
	 *
	 * @param bucket  The bucket to use.
	 * @param key     The key to use.
	 * @param retrier The {@link Retrier} to use in case a retry is required.
	 * @return A {@link Promise} that will be completed when the value has been deleted.
	 */
	public Promise<Void> delete(final Bucket bucket, final String key, final Retrier retrier) {
		final Promise<Void> p = new Promise<>(this);

		R.schedule(
				new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						DeleteObject op = bucket.delete(key);
						if (null != retrier) {
							op = op.withRetrier(retrier);
						}
						try {
							Void result = op.execute();
							if (log.isTraceEnabled()) {
								log.trace("deleted: /{}/{}", bucket.getName(), key);
							}
							Riaktor.this.notify("/" + bucket.getName() + "/" + key, new DeleteEvent(Tuple.of(bucket, key)));
							p.set(result);
						} catch (RiakException e) {
							p.set(e);
						}
					}
				},
				null,
				ioReactor
		);

		return p;
	}

	private static class BucketSelector extends BaseSelector<Bucket> {
		public BucketSelector(Bucket bucket) {
			super(bucket);
		}

		@Override
		public boolean matches(Object key) {
			if (Bucket.class.isInstance(key)) {
				return getObject().getName().equals(((Bucket) key).getName());
			} else {
				return getObject().getName().equals(key);
			}
		}
	}

	/**
	 * Events of this type are published after a value is stored. Register a {@link Consumer} on URL '/bucket/key' to be
	 * notified of these events.
	 *
	 * @param <T> The type of the data.
	 */
	public static class StoreEvent<T> extends Event<T> {
		public StoreEvent(T data) {
			super(data);
		}
	}

	/**
	 * Events of this type are published after a merge is performed. Register a {@link Consumer} on URL '/bucket/key' to be
	 * notified of these events.
	 *
	 * @param <T> The type of the data.
	 */
	public static class MergeEvent<T> extends Event<Tuple2<Collection<T>, T>> {
		public MergeEvent(Tuple2<Collection<T>, T> data) {
			super(data);
		}
	}

	/**
	 * Events of this type are published after a value is deleted. Register a {@link Consumer} on URL '/bucket/key' to be
	 * notified of these events.
	 */
	public class DeleteEvent extends Event<Tuple2<Bucket, String>> {
		public DeleteEvent(Tuple2<Bucket, String> data) {
			super(data);
		}
	}

}
