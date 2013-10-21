package com.jbrisbin.riaktor.op;

import com.jbrisbin.riaktor.spec.QuorumSpec;
import reactor.core.composable.Composable;
import reactor.function.Function;

import java.util.List;

/**
 * Encapsulates information about a CAP operation.
 *
 * @author Jon Brisbin
 */
public abstract class Operation<T, C extends Composable<T>> {

	private QuorumSpec           qSpec;

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
	 * Commit this {@code Operation} to the server.
	 *
	 * @return a {@link reactor.core.composable.Composable} representing the future value of this operation.
	 */
	public abstract C commit();

}
