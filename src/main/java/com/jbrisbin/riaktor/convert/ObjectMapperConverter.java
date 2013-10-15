package com.jbrisbin.riaktor.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.SimpleType;
import reactor.convert.Converter;

import java.io.IOException;

/**
 * @author Jon Brisbin
 */
public class ObjectMapperConverter implements Converter {

	private final ObjectMapper mapper;

	public ObjectMapperConverter() {
		this(new ObjectMapper());
	}

	public ObjectMapperConverter(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
		SimpleType simpleTargetType = SimpleType.construct(targetType);

		return (byte[].class.isAssignableFrom(sourceType) && mapper.canDeserialize(simpleTargetType))
				|| (byte[].class.isAssignableFrom(targetType) && mapper.canSerialize(sourceType));
	}

	@Override
	public <T> T convert(Object source, Class<T> targetType) {
		try {
			if(source instanceof byte[]) {
				return mapper.readValue((byte[])source, targetType);
			} else {
				return (T)mapper.writeValueAsBytes(source);
			}
		} catch(IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
