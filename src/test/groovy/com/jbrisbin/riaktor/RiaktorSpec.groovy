package com.jbrisbin.riaktor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.Environment
import reactor.core.composable.Promise
import reactor.function.Suppliers
import spock.lang.Specification
import spock.lang.Timeout

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.is
/**
 * @author Jon Brisbin
 */
class RiaktorSpec extends Specification {

	static Logger LOG = LoggerFactory.getLogger(RiaktorSpec)

	Environment env
	Riaktor riaktor

	def setup() {
		env = new Environment()
		riaktor = connect()
	}

	Riaktor connect() {
		new com.jbrisbin.riaktor.spec.RiaktorSpec().env(env).get().start().await()
	}

	@Timeout(60)
	def "modifies an object"() {

		given:
			byte[] vclock

		when:
			'an object is created'
			def e1 = riaktor.put("test", null, new Person(name: "John Doe")).
					metadata("test", "value").
					returnBody(true).
					commit().
					await()
			LOG.info "entry[1]: $e1"
			vclock = e1.headers.getVclock()

		then:
			'the object was created'
			null != e1.data?.name

		when:
			'the object is modified'
			e1.data.name = "Jane Doe"
			def e2 = riaktor.put("test", e1.key, e1.data).
					metadata("test", "value2").
					returnBody(true).
					commit().
					await()
			LOG.info "entry[2]: $e2"

		then:
			'the object is modifed'
			vclock != e2.headers.getVclock()

		when:
			'an object is fetched'
			def e3 = riaktor.get("test", e1.key, Person).
					commit().
					await()
			LOG.info "entry[3]: $e3"

		then:
			'the data is correct'
			e3.data?.name == 'Jane Doe'
			e3.headers["test"] == "value2"

	}

	@Timeout(60)
	def "performs well"() {

		given:
			def riaktors = Suppliers.roundRobin(connect(), connect())

			def runs = 10000
			def entry = riaktor.put("test", null, new Person(name: "John Doe")).
					metadata("test", "value").
					commit().
					await()
			long start = System.currentTimeMillis();
			long end
			double elapsed
			List<Promise<Entry<Person>>> promises = []

		when:
			(1..runs).each {
				promises << riaktors.get().get("test", entry.key, Person).commit()
			}
			promises.each {
				assertThat("Metadata was extracted", it.await().headers["test"], is("value"))
			}
			end = System.currentTimeMillis()
			elapsed = end - start
			LOG.info "throughput: ${Math.floor(runs / (elapsed / 1000))}/s"

		then:
			(runs / (elapsed / 1000)) > 1000
	}

}
