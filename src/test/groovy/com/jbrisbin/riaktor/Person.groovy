package com.jbrisbin.riaktor

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * @author Jon Brisbin
 */
@JsonIgnoreProperties(["metaClass"])
class Person {

	String name

	@Override
	public String toString() {
		return "Person{" +
				", name='" + name + '\'' +
				'}';
	}

}
