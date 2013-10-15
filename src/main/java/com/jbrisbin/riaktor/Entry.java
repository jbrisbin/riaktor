package com.jbrisbin.riaktor;

/**
 * @author Jon Brisbin
 */
public class Entry<T> {

	private final String  bucket;
	private final String  key;
	private final Headers headers;
	private final T       data;

	Entry(String bucket, String key, Headers headers, T data) {
		this.bucket = bucket;
		this.key = key;
		this.headers = headers;
		this.data = data;
	}

	public String getBucket() {
		return bucket;
	}

	public String getKey() {
		return key;
	}

	public Headers getHeaders() {
		return headers;
	}

	public T getData() {
		return data;
	}

	@Override
	public String toString() {
		return "Entry{" +
				"bucket='" + bucket + '\'' +
				", key='" + key + '\'' +
				", headers=" + headers +
				", data=" + data +
				'}';
	}

}
