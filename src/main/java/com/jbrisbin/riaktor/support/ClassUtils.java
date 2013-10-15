package com.jbrisbin.riaktor.support;

/**
 * @author Jon Brisbin
 */
public final class ClassUtils {

	private ClassUtils() {
	}

	public static boolean isClassPresent(String classname, ClassLoader cl) {
		try {
			return null != cl.loadClass(classname);
		} catch(ClassNotFoundException e) {
			return false;
		}
	}

}
