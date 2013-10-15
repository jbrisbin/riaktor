package com.jbrisbin.riaktor.spec;

/**
 * @author Jon Brisbin
 */
public class QuorumSpec {

	private int     pr    = Integer.MIN_VALUE;
	private int     r     = Integer.MIN_VALUE;
	private int     w     = Integer.MIN_VALUE;
	private int     pw    = Integer.MIN_VALUE;
	private int     dw    = Integer.MIN_VALUE;
	private int     rw    = Integer.MIN_VALUE;
	private int     nval  = Integer.MIN_VALUE;
	private boolean basic = true;

	public static QuorumSpec q() {
		return new QuorumSpec();
	}

	public boolean hasPr() {
		return has(pr);
	}

	public int pr() {
		return pr;
	}

	public QuorumSpec pr(int pr) {
		this.pr = pr;
		return this;
	}

	public boolean hasR() {
		return has(r);
	}

	public int r() {
		return r;
	}

	public QuorumSpec r(int r) {
		this.r = r;
		return this;
	}

	public boolean hasW() {
		return has(w);
	}

	public int w() {
		return w;
	}

	public QuorumSpec w(int w) {
		this.w = w;
		return this;
	}

	public boolean hasPw() {
		return has(pw);
	}

	public int pw() {
		return pw;
	}

	public QuorumSpec pw(int pw) {
		this.pw = pw;
		return this;
	}

	public boolean hasDw() {
		return has(dw);
	}

	public int dw() {
		return dw;
	}

	public QuorumSpec dw(int dw) {
		this.dw = dw;
		return this;
	}

	public boolean hasRw() {
		return has(rw);
	}

	public int rw() {
		return rw;
	}

	public QuorumSpec rw(int rw) {
		this.rw = rw;
		return this;
	}

	public boolean hasNval() {
		return has(nval);
	}

	public int nval() {
		return nval;
	}

	public QuorumSpec nval(int nval) {
		this.nval = nval;
		return this;
	}

	public boolean basic() {
		return basic;
	}

	public QuorumSpec basic(boolean basic) {
		this.basic = basic;
		return this;
	}

	private boolean has(int i) {
		return i != Integer.MIN_VALUE;
	}

}
