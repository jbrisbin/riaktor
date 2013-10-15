package com.jbrisbin.riaktor.event;

/**
 * @author Jon Brisbin
 */
public enum EventType {

	ErrorResp,
	PingReq,
	PingResp,
	GetClientIdReq,
	GetClientIdResp,
	SetClientIdReq,
	SetClientIdResp,
	GetServerInfoReq,
	GetServerInfoResp,
	GetReq,
	GetResp,
	PutReq,
	PutResp,
	DelReq,
	DelResp,
	ListBucketsReq,
	ListBucketsResp,
	ListKeysReq,
	ListKeysResp,
	GetBucketReq,
	GetBucketResp,
	SetBucketReq,
	SetBucketResp,
	MapRedReq,
	MapRedResp,
	IndexReq,
	IndexResp,
	SearchQueryReq,
	SearchQueryResp;

	public static EventType fromByteFlag(byte b) {
		switch(b) {
			case 0:
				return ErrorResp;
			case 1:
				return PingReq;
			case 2:
				return PingResp;
			case 3:
				return GetClientIdReq;
			case 4:
				return GetClientIdResp;
			case 5:
				return SetClientIdReq;
			case 6:
				return SetClientIdResp;
			case 7:
				return GetServerInfoReq;
			case 8:
				return GetServerInfoResp;
			case 9:
				return GetReq;
			case 10:
				return GetResp;
			case 11:
				return PutReq;
			case 12:
				return PutResp;
			case 13:
				return DelReq;
			case 14:
				return DelResp;
			case 15:
				return ListBucketsReq;
			case 16:
				return ListBucketsResp;
			case 17:
				return ListKeysReq;
			case 18:
				return ListKeysResp;
			case 19:
				return GetBucketReq;
			case 20:
				return GetBucketResp;
			case 21:
				return SetBucketReq;
			case 22:
				return SetBucketResp;
			case 23:
				return MapRedReq;
			case 24:
				return MapRedResp;
			case 25:
				return IndexReq;
			case 26:
				return IndexResp;
			case 27:
				return SearchQueryReq;
			case 28:
				return SearchQueryResp;
			default:
				throw new IllegalArgumentException(b + " not a valid message type.");
		}
	}

	public byte byteFlag() {
		switch(this) {
			case ErrorResp:
				return 0x0;
			case PingReq:
				return 0x1;
			case PingResp:
				return 0x2;
			case GetClientIdReq:
				return 0x3;
			case GetClientIdResp:
				return 0x4;
			case SetClientIdReq:
				return 0x5;
			case SetClientIdResp:
				return 0x6;
			case GetServerInfoReq:
				return 0x7;
			case GetServerInfoResp:
				return 0x8;
			case GetReq:
				return 0x9;
			case GetResp:
				return 0xA;
			case PutReq:
				return 0xB;
			case PutResp:
				return 0xC;
			case DelReq:
				return 0xD;
			case DelResp:
				return 0xE;
			case ListBucketsReq:
				return 0xF;
			case ListBucketsResp:
				return 0x10;
			case ListKeysReq:
				return 0x11;
			case ListKeysResp:
				return 0x12;
			case GetBucketReq:
				return 0x13;
			case GetBucketResp:
				return 0x14;
			case SetBucketReq:
				return 0x15;
			case SetBucketResp:
				return 0x16;
			case MapRedReq:
				return 0x17;
			case MapRedResp:
				return 0x18;
			case IndexReq:
				return 0x19;
			case IndexResp:
				return 0x1A;
			case SearchQueryReq:
				return 0x1B;
			case SearchQueryResp:
				return 0x1C;
			default:
				return -1;
		}
	}

}
