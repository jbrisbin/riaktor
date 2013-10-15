package com.jbrisbin.riaktor.event;

import reactor.event.Event;

/**
 * @author Jon Brisbin
 */
public class RiakEvent<T> extends Event<T> {

	private final EventType type;

	public RiakEvent(T obj,
	                 EventType type) {
		super(obj);
		this.type = type;
	}

	public EventType getType() {
		return type;
	}

}
