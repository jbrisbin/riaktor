package com.jbrisbin.riaktor.op;

import com.jbrisbin.riaktor.Entry;
import reactor.core.composable.Composable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public abstract class PutOperation<T, C extends Composable<Entry<T>>> extends Operation<T, C> {

	private final Map<String, String> metadata   = new HashMap<>();
	private       boolean             returnBody = true;

	private byte[] vclock;
	private String contentType;

	public String contentType() {
		return contentType;
	}

	public PutOperation<T, C> contentType(String contentType) {
		this.contentType = contentType;
		return this;
	}

	public boolean returnBody() {
		return returnBody;
	}

	public PutOperation<T, C> returnBody(boolean returnBody) {
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
	public PutOperation<T, C> vclock(byte[] vclock) {
		this.vclock = vclock;
		return this;
	}

	public PutOperation<T, C> metadata(String key, String value) {
		metadata.put(key, value);
		return this;
	}

	public Map<String, String> metadata() {
		return metadata;
	}

}
