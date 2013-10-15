package com.jbrisbin.riaktor;

import java.net.URI;

/**
 * @author Jon Brisbin
 */
public class Link {

	private final String bucket;
	private final String key;
	private final String tag;

	public Link(String bucket, String key, String tag) {
		this.bucket = bucket;
		this.key = key;
		this.tag = tag;
	}

	public String getBucket() {
		return (null != bucket ? bucket : "");
	}

	public String getKey() {
		return (null != key ? key : "");
	}

	public String getTag() {
		return (null != tag ? tag : "");
	}

	public URI toURI() {
		return URI.create(
				String.format("/%s/%s?tag=%s", getBucket(), getKey(), getTag())
		);
	}

}
