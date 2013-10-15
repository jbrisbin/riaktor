package com.jbrisbin.riaktor.spec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jbrisbin.riaktor.Riaktor;
import com.jbrisbin.riaktor.convert.ObjectMapperConverter;
import reactor.convert.Converter;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.support.EventRoutingComponentSpec;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class RiaktorSpec extends EventRoutingComponentSpec<RiaktorSpec, Riaktor> {

	private       int                    timeout            = 60000;
	private final Map<String, Converter> converters         = new LinkedHashMap<>();
	private final List<URI>              connectURIs        = new ArrayList<>();
	private       String                 defaultContentType = "application/json";

	public RiaktorSpec timeout(int timeoutInMilliseconds) {
		this.timeout = timeoutInMilliseconds;
		return this;
	}

	public RiaktorSpec connect(String host) {
		return connect(host, 8087);
	}

	public RiaktorSpec connect(String host, int port) {
		connectURIs.add(URI.create(String.format("riakpb://%s:%s", host, port)));
		return this;
	}

	public RiaktorSpec converter(String contentType, Converter converter) {
		converters.put(contentType, converter);
		return this;
	}

	public RiaktorSpec defaultContentType(String contentType) {
		this.defaultContentType = contentType;
		return this;
	}

	@Override
	protected Riaktor configure(Reactor reactor, Environment environment) {
		if(!converters.containsKey("application/json")) {
			ObjectMapper mapper = new ObjectMapper();
			mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
			mapper.enable(SerializationFeature.INDENT_OUTPUT);
			converters.put("application/json", new ObjectMapperConverter(mapper));
		}

		return new Riaktor(environment,
		                   reactor,
		                   timeout,
		                   connectURIs,
		                   converters,
		                   defaultContentType);
	}

}
