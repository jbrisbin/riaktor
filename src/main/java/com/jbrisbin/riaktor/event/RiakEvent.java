package com.jbrisbin.riaktor.event;

import reactor.event.Event;

/**
 * @author Jon Brisbin
 */
public class RiakEvent<T> extends Event<T> {

	private static final long serialVersionUID = 5936580452183790570L;
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
