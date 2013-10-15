package com.jbrisbin.riaktor.encoding;

import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.basho.riak.protobuf.RiakSearchPB;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.jbrisbin.riaktor.event.EventType;
import com.jbrisbin.riaktor.event.RiakEvent;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;

/**
 * @author Jon Brisbin
 */
public class RpbCodec implements Codec<Buffer, Message, RiakEvent<Message>> {
	@Override
	public Function<Buffer, Message> decoder(final Consumer<Message> next) {
		return new Function<Buffer, Message>() {
			@Override
			public Message apply(Buffer buffer) {
				while(buffer.remaining() > 0) {
					EventType type = EventType.fromByteFlag(buffer.read());
					byte[] msgbytes = buffer.asBytes();
					buffer.skip(msgbytes.length);

					Message msg = null;
					try {
						switch(type) {
							case ErrorResp:
								msg = RiakPB.RpbGetServerInfoResp.parseFrom(msgbytes);
								break;
							case GetClientIdResp:
								msg = RiakKvPB.RpbGetClientIdResp.parseFrom(msgbytes);
								break;
							case GetServerInfoResp:
								msg = RiakPB.RpbGetServerInfoResp.parseFrom(msgbytes);
								break;
							case GetResp:
								msg = RiakKvPB.RpbGetResp.parseFrom(msgbytes);
								break;
							case PutResp:
								msg = RiakKvPB.RpbPutResp.parseFrom(msgbytes);
								break;
							case ListBucketsResp:
								msg = RiakKvPB.RpbListBucketsResp.parseFrom(msgbytes);
								break;
							case ListKeysResp:
								msg = RiakKvPB.RpbListKeysResp.parseFrom(msgbytes);
								break;
							case GetBucketResp:
								msg = RiakPB.RpbGetBucketResp.parseFrom(msgbytes);
								break;
							case MapRedResp:
								msg = RiakKvPB.RpbMapRedResp.parseFrom(msgbytes);
								break;
							case IndexResp:
								msg = RiakKvPB.RpbIndexResp.parseFrom(msgbytes);
								break;
							case SearchQueryResp:
								msg = RiakSearchPB.RpbSearchQueryResp.parseFrom(msgbytes);
								break;
						}

						if(null != next) {
							next.accept(msg);
						} else {
							return msg;
						}
					} catch(InvalidProtocolBufferException e) {
						throw new IllegalStateException(e);
					}
				}

				return null;
			}
		};
	}

	@Override
	public Function<RiakEvent<Message>, Buffer> encoder() {
		return new Function<RiakEvent<Message>, Buffer>() {
			@Override
			public Buffer apply(RiakEvent<Message> ev) {
				EventType type = ev.getType();
				Message outmsg = ev.getData();

				byte[] msgbytes = (null != outmsg ? outmsg.toByteArray() : new byte[0]);
				int len = msgbytes.length + 1;
				Buffer buff = new Buffer().append(type.byteFlag());
				if(len > 0) {
					buff.append(msgbytes);
				}

				return buff.flip();
			}
		};
	}
}
