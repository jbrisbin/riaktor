package com.jbrisbin.riaktor.op;

import com.jbrisbin.riaktor.Entry;
import com.jbrisbin.riaktor.event.RiakEvent;
import com.jbrisbin.riaktor.spec.QuorumSpec;
import reactor.core.composable.Composable;
import reactor.event.Event;
import reactor.function.Function;

import java.util.List;

/**
 * Encapsulates information about a CAP operation.
 *
 * @author Jon Brisbin
 */
public abstract class Operation<T, C extends Composable<Entry<T>>> {

	private QuorumSpec           qSpec;
	private Function<List<T>, T> conflictResolver;
	private String               etag;

	/**
	 * Set the {@link QuorumSpec} to define what quorum values should be used during this request.
	 *
	 * @param qSpec
	 *
	 * @return {@literal this}
	 */
	public Operation<T, C> quorum(QuorumSpec qSpec) {
		this.qSpec = qSpec;
		return this;
	}

	/**
	 * Get the {@link QuorumSpec} to define what quorum values should be used during this request.
	 *
	 * @return the {@link QuorumSpec} to use.
	 */
	public QuorumSpec quorum() {
		return qSpec;
	}

	/**
	 * Set the conflict resolver to use to resolve sibling conflicts.
	 *
	 * @param conflictResolver
	 *
	 * @return {@literal this}
	 */
	public Operation<T, C> conflictResolver(Function<List<T>, T> conflictResolver) {
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
	public Operation<T, C> ifNoneMatch(String etag) {
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

	/**
	 * Commit this {@code Operation} to the server.
	 *
	 * @return a {@link reactor.core.composable.Composable} representing the future value of this operation.
	 */
	public abstract C commit();

}
