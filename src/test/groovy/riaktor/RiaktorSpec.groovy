package riaktor

import com.basho.riak.client.bucket.Bucket
import reactor.fn.Deferred
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.lessThan

/**
 * @author Jon Brisbin
 */
class RiaktorSpec extends Specification {

	Riaktor riaktor

	def setup() {
		riaktor = new Riaktor()
	}

	def "Riaktor fetches buckets"() {

		when: "a fetchBucket call"
		def p = riaktor.fetchBucket("test")

		then: "value is a Bucket and its name is 'test'"
		Bucket.class.isInstance(await(p, 1))
		p.get().name == 'test'

	}

	def "Riaktor stores data"() {

		given: "data"
		def data = "${System.currentTimeMillis()}".toString()

		when: "a value is stored"
		def p = riaktor.fetchBucket("test").map({ Bucket b ->
			await(riaktor.store(b, "test", data), 5)
		})

		then: "value is stored"
		await(p, 5) == null

	}

	def "Riaktor fetches data"() {

		given: "a test Bucket"
		def b = await(riaktor.fetchBucket("test"), 5)

		when: "a value is fetched"
		def p = riaktor.fetch(b, "test")

		then: "a non-null value is retrieved"
		await(p, 5) != null

	}

	def "Riaktor deletes data"() {

		when: "a value is deleted"
		def b = await(riaktor.fetchBucket("test"), 5)
		def p = riaktor.delete(b, "test")

		then: "the value was deleted"
		await(p, 5) == null

		when: "the value is fetched again"
		p = riaktor.fetch(b, "test")

		then: "the value is null"
		await(p, 5) == null

	}

	def "use Bucket to create a StoreObject"() {

		given: "a test Bucket"
		def b = await(riaktor.fetchBucket("test"), 5)

		when: "a StoreObject is created"
		def op = b.store("test", "Hello World!".toString()).w(1).dw(1)
		def p = riaktor.send(op)

		then: "the operation is executed"
		await(p, 5) == null

	}

	private <T> T await(Deferred<T> d, long seconds) {
		def start = System.currentTimeMillis()
		T result = d.await(seconds, TimeUnit.SECONDS)
		def end = System.currentTimeMillis()
		assertThat("await hasn't timed out", (end - start), lessThan(seconds * 1000));
		return result;
	}

}
