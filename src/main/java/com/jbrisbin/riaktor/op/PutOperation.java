package com.jbrisbin.riaktor.op;

import com.jbrisbin.riaktor.Entry;
import reactor.core.composable.Promise;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public abstract class PutOperation<T> extends Operation<Entry<T>, Promise<Entry<T>>> {

	private final Map<String, String> metadata   = new HashMap<>();
	private       boolean             returnBody = true;

	private String etag;
	private byte[] vclock;
	private String contentType;

	public String contentType() {
		return contentType;
	}

	public PutOperation<T> contentType(String contentType) {
		this.contentType = contentType;
		return this;
	}

	public boolean returnBody() {
		return returnBody;
	}

	public PutOperation<T> returnBody(boolean returnBody) {
		this.returnBody = returnBody;
		return this;
	}

	/**
	 * Get the vclock to use on updates.
	 *
	 * @return the vclock to use.
	 */
	public byte[] vclock() {
		return vclock;
	}

	/**
	 * Set the vclock to use on updates.
	 *
	 * @param vclock
	 *
	 * @return {@literal this}
	 */
	public PutOperation<T> vclock(byte[] vclock) {
		this.vclock = vclock;
		return this;
	}

	public PutOperation<T> metadata(String key, String value) {
		metadata.put(key, value);
		return this;
	}

	public Map<String, String> metadata() {
		return metadata;
	}

	/**
	 * Specify an entity tag for operations that use If-None-Match semantics.
	 *
	 * @param etag
	 * 		the entity tag to use
	 *
	 * @return {@literal this}
	 */
	public PutOperation<T> ifNoneMatch(String etag) {
		this.etag = etag;
		return this;
	}

	/**
	 * Get the entity tag for operations that use If-None-Match semantics.
	 *
	 * @return the entity tag to use
	 */
	public String ifNoneMatch() {
		return etag;
	}

}
