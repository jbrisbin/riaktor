package com.jbrisbin.riaktor;

import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.jbrisbin.riaktor.encoding.RpbCodec;
import com.jbrisbin.riaktor.event.EventType;
import com.jbrisbin.riaktor.event.RiakEvent;
import com.jbrisbin.riaktor.op.DeleteOperation;
import com.jbrisbin.riaktor.op.GetOperation;
import com.jbrisbin.riaktor.op.ListKeysOperation;
import com.jbrisbin.riaktor.op.PutOperation;
import com.jbrisbin.riaktor.spec.QuorumSpec;
import com.jbrisbin.riaktor.support.TypeMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.convert.Converter;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.queue.BlockingQueueFactory;
import reactor.tcp.Reconnect;
import reactor.tcp.TcpClient;
import reactor.tcp.TcpConnection;
import reactor.tcp.config.ClientSocketOptions;
import reactor.tcp.encoding.LengthFieldCodec;
import reactor.tcp.netty.NettyTcpClient;
import reactor.tcp.spec.TcpClientSpec;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;
import reactor.tuple.Tuple3;
import reactor.util.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

import static reactor.event.selector.Selectors.T;

/**
 * @author Jon Brisbin
 */
public class Riaktor implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(Riaktor.class);

	private final Environment                            env;
	private final Dispatcher                             dispatcher;
	private final int                                    timeout;
	private final Reactor                                reactor;
	private final ByteString                             defaultContentType;
	private final Reconnect                              reconnect;
	private final TcpClient<Message, RiakEvent<Message>> client;
	private final Queue<RiakEvent>                       requestQueue;
	private final Queue<Deferred>                        responseQueue;

	private final List<Tuple3<String, String, Converter>> converters
			= new ArrayList<>();
	private final Map<ByteString, Converter>              converterCache
			= Collections.synchronizedMap(new HashMap<ByteString, Converter>());

	private volatile TcpConnection<Message, RiakEvent<Message>> connection;

	public Riaktor(Environment env,
	               Reactor reactor,
	               int timeout,
	               List<URI> connectURIs,
	               Map<String, Converter> converters,
	               String defaultContentType) {
		Assert.notNull(env, "Environment cannot be null.");
		Assert.notNull(reactor, "Reactor cannot be null.");
		Assert.notNull(converters, "Converters cannot be null.");
		this.env = env;
		this.reactor = reactor;
		this.dispatcher = reactor.getDispatcher();
		this.timeout = timeout;
		this.defaultContentType = ByteString.copyFromUtf8(defaultContentType);
		this.requestQueue = BlockingQueueFactory.createQueue();
		this.responseQueue = BlockingQueueFactory.createQueue();

		for(final Map.Entry<String, Converter> entry : converters.entrySet()) {
			String[] parts = entry.getKey().split("/");
			Assert.isTrue(parts.length == 2, "Invalid RFC 1590 Content-Type pattern: " + entry.getKey());

			this.converters.add(Tuple.<String, String, Converter>of(
					parts[0],
					parts[1],
					entry.getValue())
			);

		}

		this.reactor.on(T(Throwable.class), new Consumer<Event<Throwable>>() {
			@Override
			public void accept(Event<Throwable> ev) {
				if(null == responseQueue.peek()) {
					if(LOG.isWarnEnabled()) {
						LOG.warn("Error received but no request waiting on response: {}", ev.getData());
					}
					return;
				}
				responseQueue.remove().accept(ev.getData());
			}
		});

		InetSocketAddress connectAddr;
		if(null == connectURIs || connectURIs.isEmpty()) {
			connectAddr = new InetSocketAddress("localhost", 8087);
			reconnect = null;
		} else {
			URI firstUri = connectURIs.get(0);
			connectAddr = new InetSocketAddress(firstUri.getHost(), firstUri.getPort());

			final int len = connectURIs.size();
			final List<InetSocketAddress> addresses = new ArrayList<>(len);
			for(URI u : connectURIs) {
				addresses.add(new InetSocketAddress(u.getHost(), u.getPort()));
			}

			reconnect = new Reconnect() {
				@Override
				public Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt) {
					InetSocketAddress nextAddr = addresses.get(attempt % len);
					long timeout = (attempt == 0 ? 0 : (long)Riaktor.this.timeout);
					if(LOG.isInfoEnabled()) {
						LOG.info("Reconnecting to {} after {}", nextAddr, (attempt == 0 ? 0 : (long)Riaktor.this.timeout));
					}
					return Tuple.of(nextAddr, timeout);
				}
			};
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("\t  Connect:\t{}", connectAddr);
			LOG.debug("\tReconnect:\t{}", reconnect);
			LOG.debug("\t  Timeout:\t{}", timeout);
		}

		client = new TcpClientSpec<Message, RiakEvent<Message>>(NettyTcpClient.class)
				.env(env)
				.dispatcher(dispatcher)
				.codec(new LengthFieldCodec<>(new RpbCodec()))
				.options(new ClientSocketOptions().timeout(timeout))
				.connect(connectAddr)
				.get();
	}

	public Registration<Consumer<Event<Throwable>>> when(Class<Throwable> type,
	                                                     Consumer<Event<Throwable>> consumer) {
		return reactor.on(T(type), consumer);
	}

	public Promise<Riaktor> start() {
		if(null != connection) {
			throw new IllegalStateException("This Riaktor has already been started.");
		}

		final Consumer<Message> msgConsumer = new Consumer<Message>() {
			@SuppressWarnings("unchecked")
			@Override
			public void accept(Message msg) {
				if(null == responseQueue.peek()) {
					return;
				}

				Deferred d = responseQueue.remove();
				if(msg instanceof RiakPB.RpbErrorResp) {
					d.accept(new IllegalStateException(((RiakPB.RpbErrorResp)msg).getErrmsg()
					                                                             .toStringUtf8()));
				} else {
					d.accept(msg);
				}
			}
		};

		if(null == reconnect) {
			return client.open()
			             .onError(new Consumer<Throwable>() {
				             @Override
				             public void accept(Throwable t) {
					             reactor.notify(t.getClass(), Event.wrap(t));
				             }
			             })
			             .onSuccess(new Consumer<TcpConnection<Message, RiakEvent<Message>>>() {
				             @Override
				             public void accept(final TcpConnection<Message, RiakEvent<Message>> conn) {
					             synchronized(client) {
						             Riaktor.this.connection = conn;
						             client.notifyAll();
					             }

					             drainRequestQueue();

					             conn.on().close(new Runnable() {
						             @Override
						             public void run() {
							             synchronized(client) {
								             Riaktor.this.connection = null;
							             }
							             if(LOG.isDebugEnabled()) {
								             LOG.debug("Connection closed {}", conn);
							             }
						             }
					             });
					             conn.consume(msgConsumer);
				             }
			             })
			             .map(new Function<TcpConnection<Message, RiakEvent<Message>>, Riaktor>() {
				             @Override
				             public Riaktor apply(TcpConnection<Message, RiakEvent<Message>> conn) {
					             return Riaktor.this;
				             }
			             });
		} else {
			final Deferred<TcpConnection<Message, RiakEvent<Message>>, Promise<TcpConnection<Message, RiakEvent<Message>>>> d
					= promise();

			client.open(reconnect)
			      .consume(new Consumer<TcpConnection<Message, RiakEvent<Message>>>() {
				      @Override
				      public void accept(TcpConnection<Message, RiakEvent<Message>> conn) {
					      synchronized(client) {
						      Riaktor.this.connection = conn;
						      client.notifyAll();
					      }

					      conn.on().close(new Runnable() {
						      @Override
						      public void run() {
							      synchronized(client) {
								      Riaktor.this.connection = null;
							      }
						      }
					      });
					      conn.consume(msgConsumer);

					      d.accept(conn);

					      drainRequestQueue();
				      }
			      });

			return d.compose().map(new Function<TcpConnection<Message, RiakEvent<Message>>, Riaktor>() {
				@Override
				public Riaktor apply(TcpConnection<Message, RiakEvent<Message>> conn) {
					return Riaktor.this;
				}
			});
		}
	}

	@SuppressWarnings("unchecked")
	public <T> PutOperation<T> put(final String bucket,
	                               final String key,
	                               final T obj) {
		Assert.notNull(bucket, "Bucket cannot be null.");
		Assert.notNull(obj, "Object cannot be null.");

		final Class<T> type = (Class<T>)obj.getClass();
		final Deferred<RiakKvPB.RpbPutResp, Promise<RiakKvPB.RpbPutResp>> d = promise();
		final RiakKvPB.RpbPutReq.Builder b = RiakKvPB.RpbPutReq.newBuilder()
		                                             .setTimeout(timeout)
		                                             .setBucket(ByteString.copyFromUtf8(bucket));
		if(null != key) {
			b.setKey(ByteString.copyFromUtf8(key));
		}

		final TypeMeta typeMeta = TypeMeta.fromType(obj.getClass());
		final byte[] vclock = typeMeta.vclockSupplier(obj).get();

		return new PutOperation<T>() {
			@Override
			public Promise<Entry<T>> commit() {
				if(!returnBody()) {
					b.setReturnBody(false);
					b.setReturnHead(true);
				} else {
					b.setReturnBody(true);
				}

				QuorumSpec q = quorum();
				if(null != q) {
					if(q.hasW()) {
						b.setW(q.w());
					}
					if(q.hasDw()) {
						b.setDw(q.dw());
					}
					if(q.hasPw()) {
						b.setPw(q.pw());
					}
					if(q.hasNval()) {
						b.setNVal(q.nval());
					}
					if(null != ifNoneMatch()) {
						b.setIfNoneMatch(true);
					}
				}

				if(null != vclock()) {
					b.setVclock(ByteString.copyFrom(vclock()));
				} else if(null != vclock) {
					b.setVclock(ByteString.copyFrom(vclock));
				}

				RiakKvPB.RpbContent.Builder content = RiakKvPB.RpbContent.newBuilder();
				ByteString contentType = defaultContentType;
				if(null != contentType()) {
					contentType = ByteString.copyFromUtf8(contentType());
				}
				content.setContentType(contentType);

				Converter c;
				byte[] contentBytes;
				if(type != byte[].class && null != (c = converterFor(contentType))) {
					contentBytes = c.convert(obj, byte[].class);
				} else {
					contentBytes = (byte[])obj;
				}
				content.setValue(ByteString.copyFrom(contentBytes));

				Map<String, String> metadata = metadata();
				Map<String, String> objMeta = typeMeta.metadataSupplier(obj).get();
				if(null != objMeta && !objMeta.isEmpty()) {
					metadata.putAll(objMeta);
				}
				if(!metadata.isEmpty()) {
					for(Map.Entry<String, String> entry : metadata.entrySet()) {
						content.addUsermeta(RiakPB.RpbPair.newBuilder()
						                          .setKey(ByteString.copyFromUtf8(entry.getKey()))
						                          .setValue(ByteString.copyFromUtf8(entry.getValue()))
						                          .build());
					}
				}

				b.setContent(content.build());

				request(d, new RiakEvent<>(b.build(), EventType.PutReq));

				return d.compose().map(new Function<RiakKvPB.RpbPutResp, Entry<T>>() {
					@Override
					public Entry<T> apply(RiakKvPB.RpbPutResp rpbPutResp) {
						String skey;
						if(null == key) {
							skey = rpbPutResp.getKey().toStringUtf8();
						} else {
							skey = key;
						}
						return new ResponseMapFunction<RiakKvPB.RpbPutResp, T>(
								rpbPutResp.getContentList(),
								type,
								null,
								rpbPutResp.getVclock().toByteArray(),
								bucket,
								skey
						).apply(rpbPutResp);
					}
				});
			}
		};
	}

	public <T> GetOperation<T> get(final String bucket,
	                               final String key,
	                               final Class<T> asType) {
		Assert.notNull(bucket, "Bucket cannot be null.");
		Assert.notNull(key, "Key cannot be null.");

		final Deferred<RiakKvPB.RpbGetResp, Promise<RiakKvPB.RpbGetResp>> d = promise();
		final RiakKvPB.RpbGetReq.Builder b = RiakKvPB.RpbGetReq.newBuilder()
		                                             .setTimeout(timeout)
		                                             .setBucket(ByteString.copyFromUtf8(bucket))
		                                             .setKey(ByteString.copyFromUtf8(key));

		return new GetOperation<T>() {
			@Override
			public Promise<Entry<T>> commit() {
				QuorumSpec q = quorum();
				if(null != q) {
					if(q.hasR()) {
						b.setR(q.r());
					}
					if(q.hasPr()) {
						b.setPr(q.pr());
					}
					if(q.hasNval()) {
						b.setNVal(q.nval());
					}
					b.setBasicQuorum(q.basic());
				}

				request(d, new RiakEvent<>(b.build(), EventType.GetReq));

				return d.compose().map(new Function<RiakKvPB.RpbGetResp, Entry<T>>() {
					@Override
					public Entry<T> apply(RiakKvPB.RpbGetResp rpbGetResp) {
						return new ResponseMapFunction<RiakKvPB.RpbGetResp, T>(
								rpbGetResp.getContentList(),
								asType,
								conflictResolver(),
								rpbGetResp.getVclock().toByteArray(),
								bucket,
								key
						).apply(rpbGetResp);
					}
				});
			}
		};
	}

	public DeleteOperation delete(final String bucket,
	                              final String key) {
		Assert.notNull(bucket, "Bucket cannot be null.");
		Assert.notNull(key, "Key cannot be null.");

		final Deferred<Message, Promise<Message>> d = promise();
		final RiakKvPB.RpbDelReq.Builder b = RiakKvPB.RpbDelReq.newBuilder()
		                                             .setTimeout(timeout)
		                                             .setBucket(ByteString.copyFromUtf8(bucket))
		                                             .setKey(ByteString.copyFromUtf8(key));

		return new DeleteOperation() {
			@Override
			public Promise<Void> commit() {
				QuorumSpec q = quorum();
				if(null != q) {
					if(q.hasW()) {
						b.setW(q.w());
					}
					if(q.hasDw()) {
						b.setDw(q.dw());
					}
					if(q.hasR()) {
						b.setR(q.r());
					}
					if(q.hasRw()) {
						b.setRw(q.rw());
					}
					if(q.hasPr()) {
						b.setPr(q.pr());
					}
					if(q.hasPw()) {
						b.setPw(q.pw());
					}
					if(q.hasNval()) {
						b.setNVal(q.nval());
					}
				}

				request(d, new RiakEvent<>(b.build(), EventType.DelReq));

				return d.compose().map(new Function<Message, Void>() {
					@Override
					public Void apply(Message msg) {
						return null;
					}
				});
			}
		};
	}

	public ListKeysOperation listKeys(final String bucket) {
		Assert.notNull(bucket, "Bucket cannot be null.");

		final Deferred<RiakKvPB.RpbListKeysResp, Promise<RiakKvPB.RpbListKeysResp>> d = promise();
		final RiakKvPB.RpbListKeysReq.Builder b = RiakKvPB.RpbListKeysReq.newBuilder()
		                                                  .setTimeout(timeout)
		                                                  .setBucket(ByteString.copyFromUtf8(bucket));

		return new ListKeysOperation() {
			@Override
			public Promise<List<String>> commit() {
				request(d, new RiakEvent<>(b.build(), EventType.ListKeysReq));

				return d.compose().map(new Function<RiakKvPB.RpbListKeysResp, List<String>>() {
					@Override
					public List<String> apply(RiakKvPB.RpbListKeysResp rpbListKeysResp) {
						List<String> l = new ArrayList<>();
						for(ByteString byteString : rpbListKeysResp.getKeysList()) {
							l.add(byteString.toStringUtf8());
						}
						return l;
					}
				});
			}
		};
	}

	@Override
	public void close() throws IOException {
		if(null != client) {
			client.close();
		}
	}

	private <T> Deferred<T, Promise<T>> promise() {
		return Promises.<T>defer().env(env).synchronousDispatcher().get();
	}

	private <T> Deferred<T, Stream<T>> stream() {
		return Streams.<T>defer().env(env).synchronousDispatcher().get();
	}

	private void request(Deferred d, RiakEvent ev) {
		if(null != d) {
			responseQueue.add(d);
		}

		synchronized(client) {
			if(null != connection) {
				connection.send(ev);
			} else {
				if(LOG.isWarnEnabled()) {
					LOG.warn("Connection not established. Queueing request.");
				}
				requestQueue.add(ev);
			}
		}
	}

	private void drainRequestQueue() {
		synchronized(client) {
			if(null == requestQueue.peek()) {
				return;
			}
			RiakEvent ev;
			while(null != (ev = requestQueue.remove())) {
				request(null, ev);
			}
		}
	}

	private Converter converterFor(ByteString contentType) {
		Converter converter = converterCache.get(contentType);
		if(null == converter) {
			String[] parts = contentType.toStringUtf8().split("/");
			for(Tuple3<String, String, Converter> tup : converters) {
				if(tup.getT1().equals(parts[0]) || "*".equals(tup.getT1())) {
					if(parts[1].contains("+")) {
						if(parts[1].endsWith(tup.getT2())) {
							converter = tup.getT3();
							break;
						}
					} else {
						if(tup.getT2().equals(parts[1]) || "*".equals(tup.getT2())) {
							converter = tup.getT3();
							break;
						}
					}
				}
			}
			if(null == converter) {
				throw new IllegalArgumentException("Cannot find a converter for Content-Type " + contentType.toStringUtf8());
			}
			converterCache.put(contentType, converter);
		}
		return converter;
	}

	private class ResponseMapFunction<M, T> implements Function<M, Entry<T>> {
		private final List<RiakKvPB.RpbContent> contents;
		private final Class<T>                  asType;
		private final TypeMeta                  typeMeta;
		private final Function<List<T>, T>      conflictResolver;
		private final byte[]                    vclock;
		private final String                    bucket;
		private final String                    key;

		private ResponseMapFunction(List<RiakKvPB.RpbContent> contents,
		                            Class<T> asType,
		                            Function<List<T>, T> conflictResolver,
		                            byte[] vclock,
		                            String bucket,
		                            String key) {
			this.contents = contents;
			this.asType = asType;
			this.typeMeta = TypeMeta.fromType(asType);
			this.conflictResolver = conflictResolver;
			this.vclock = vclock;
			this.bucket = bucket;
			this.key = key;
		}

		@Override
		public Entry<T> apply(M msg) {
			int len = contents.size();
			List<T> objs = new ArrayList<>(len);
			List<Headers> headers = new ArrayList<>(len);
			for(RiakKvPB.RpbContent content : contents) {
				T obj;
				Converter c;
				if(null != asType
						&& asType != byte[].class
						&& null != (c = converterFor(content.getContentType()))) {
					obj = c.convert(content.getValue().toByteArray(), asType);
				} else {
					obj = (T)content.getValue().toByteArray();
				}

				if(null == obj) {
					continue;
				}

				objs.add(obj);
				headers.add(Headers.from(vclock, content));
			}

			T obj;
			if(objs.isEmpty()) {
				obj = null;
			} else if(objs.size() > 1) {
				if(null == conflictResolver) {
					throw new IllegalStateException("Siblings found but no conflict resolver set.");
				}
				obj = conflictResolver.apply(objs);
			} else {
				obj = objs.get(0);
			}

			if(null == obj) {
				return null;
			}

			Headers hdrs = null;
			if(!headers.isEmpty() && headers.size() == 1) {
				hdrs = headers.get(0);
			}

			typeMeta.metadataConsumer(obj).accept(hdrs);
			typeMeta.vclockConsumer(obj).accept(vclock);
			typeMeta.keyConsumer(obj).accept(key);

			return new Entry<>(bucket, key, hdrs, obj);
		}
	}

}
