package com.jbrisbin.riaktor;

import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;

import java.util.*;

/**
 * @author Jon Brisbin
 */
public final class Headers extends AbstractMap<String, String> {

	private final Set<Entry<String, String>> entries;
	private final List<Link>                 links;
	private final byte[]                     vclock;
	private final long                       lastModified;

	Headers(Set<Entry<String, String>> entries,
	        List<Link> links,
	        byte[] vclock,
	        long lastModified) {
		this.entries = entries;
		this.links = links;
		this.vclock = vclock;
		this.lastModified = lastModified;
	}

	static Headers from(byte[] vclock,
	                    RiakKvPB.RpbContent content) {
		if(null == content) {
			return new Headers(null, null, null, -1L);
		}
		Set<Entry<String, String>> entries = new LazyRpbPairEntrySet(content.getUsermetaList());

		List<Link> links = new ArrayList<>();
		for(RiakKvPB.RpbLink link : content.getLinksList()) {
			links.add(new Link(link.getBucket().toStringUtf8(),
			                   link.getKey().toStringUtf8(),
			                   link.getTag().toStringUtf8()));
		}

		return new Headers(entries,
		                   links,
		                   vclock,
		                   content.getLastMod());
	}

	public List<Link> getLinks() {
		return links;
	}

	public byte[] getVclock() {
		return vclock;
	}

	public long getLastModified() {
		return lastModified;
	}

	@Override
	public Set<Entry<String, String>> entrySet() {
		return entries;
	}

	@Override
	public String toString() {
		return "Headers{" +
				"entries=" + entries +
				", links=" + links +
				", vclock=byte[" + vclock.length + "]" +
				", lastModified=" + lastModified +
				'}';
	}

	private static class LazyRpbPairEntrySet extends AbstractSet<Entry<String, String>> {
		private final List<RiakPB.RpbPair> pairs;

		private LazyRpbPairEntrySet(List<RiakPB.RpbPair> pairs) {
			this.pairs = pairs;
		}

		@Override
		public Iterator<Entry<String, String>> iterator() {
			return new Iterator<Entry<String, String>>() {
				Iterator<RiakPB.RpbPair> iter = pairs.iterator();

				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Entry<String, String> next() {
					final RiakPB.RpbPair pair = iter.next();

					return new Entry<String, String>() {
						@Override
						public String getKey() {
							return pair.getKey().toStringUtf8();
						}

						@Override
						public String getValue() {
							return pair.getValue().toStringUtf8();
						}

						@Override
						public String setValue(String value) {
							throw new IllegalStateException("This Iterator is read-only.");
						}

						public String toString() {
							return getKey() + ": " + getValue();
						}
					};
				}

				@Override
				public void remove() {
					throw new IllegalStateException("This Iterator is read-only.");
				}
			};
		}

		@Override
		public int size() {
			return 0;
		}
	}
}
