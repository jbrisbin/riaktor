package com.jbrisbin.riaktor.op;

import com.jbrisbin.riaktor.Entry;
import reactor.core.composable.Promise;
import reactor.function.Function;

import java.util.List;

/**
 * @author Jon Brisbin
 */
public abstract class GetOperation<T> extends Operation<Entry<T>, Promise<Entry<T>>> {

	private Function<List<T>, T> conflictResolver;
	private String               etag;

	/**
	 * Set the conflict resolver to use to resolve sibling conflicts.
	 *
	 * @param conflictResolver
	 *
	 * @return {@literal this}
	 */
	public GetOperation<T> conflictResolver(Function<List<T>, T> conflictResolver) {
		this.conflictResolver = conflictResolver;
		return this;
	}

	/**
	 * Get the conflict resolver to use to resolve sibling conflicts.
	 *
	 * @return the conflict resolver to use.
	 */
	public Function<List<T>, T> conflictResolver() {
		return conflictResolver;
	}

	/**
	 * Specify an entity tag for operations that use If-None-Match semantics.
	 *
	 * @param etag
	 * 		the entity tag to use
	 *
	 * @return {@literal this}
	 */
	public GetOperation<T> ifNoneMatch(String etag) {
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
