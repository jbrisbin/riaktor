package com.jbrisbin.riaktor.op;

import com.jbrisbin.riaktor.Entry;
import reactor.core.composable.Composable;

/**
 * @author Jon Brisbin
 */
public abstract class GetOperation<T, C extends Composable<Entry<T>>> extends Operation<T, C> {

}
