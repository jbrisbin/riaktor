package com.jbrisbin.riaktor

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.jbrisbin.riaktor.annotation.Key
/**
 * @author Jon Brisbin
 */
@JsonIgnoreProperties(["key", "metaClass"])
class Person {

	@Key
	String key

	String name

	@Override
	public String toString() {
		return "Person{" +
				"key='" + key + '\'' +
				", name='" + name + '\'' +
				'}';
	}

}
