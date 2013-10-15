package com.jbrisbin.riaktor.support;

import com.jbrisbin.riaktor.annotation.Key;
import com.jbrisbin.riaktor.annotation.Metadata;
import com.jbrisbin.riaktor.annotation.Vclock;
import reactor.function.Consumer;
import reactor.function.Supplier;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * @author Jon Brisbin
 */
public final class TypeMeta {

	private static final Map<Class<?>, TypeMeta> typeMetaCache = Collections.synchronizedMap(
			new HashMap<Class<?>, TypeMeta>()
	);
	private Field  metadataField;
	private Method metadataSetter;
	private Method metadataGetter;
	private Field  vclockField;
	private Method vclockSetter;
	private Method vclockGetter;
	private Field  keyField;
	private Method keySetter;
	private Method keyGetter;

	private TypeMeta(Class<?> type) {
		findFields(type);
		findMethods(type);
	}

	public static TypeMeta fromType(Class<?> type) {
		TypeMeta meta = typeMetaCache.get(type);
		if(null == meta) {
			meta = new TypeMeta(type);
			typeMetaCache.put(type, meta);
		}
		return meta;
	}

	public Consumer<Map<String, String>> metadataConsumer(final Object obj) {
		return new Consumer<Map<String, String>>() {
			@Override
			public void accept(Map<String, String> m) {
				TypeMeta.set(obj, m, metadataField, metadataSetter);
			}
		};
	}

	public Supplier<Map<String, String>> metadataSupplier(final Object obj) {
		return new Supplier<Map<String, String>>() {
			@Override
			public Map<String, String> get() {
				return TypeMeta.get(obj, metadataField, metadataGetter);
			}
		};
	}

	public Consumer<byte[]> vclockConsumer(final Object obj) {
		return new Consumer<byte[]>() {
			@Override
			public void accept(byte[] bytes) {
				TypeMeta.set(obj, bytes, vclockField, vclockSetter);
			}
		};
	}

	public Supplier<byte[]> vclockSupplier(final Object obj) {
		return new Supplier<byte[]>() {
			@Override
			public byte[] get() {
				return TypeMeta.get(obj, vclockField, vclockGetter);
			}
		};
	}

	public Consumer<String> keyConsumer(final Object obj) {
		return new Consumer<String>() {
			@Override
			public void accept(String s) {
				TypeMeta.set(obj, s, keyField, keySetter);
			}
		};
	}

	public Supplier<String> keySupplier(final Object obj) {
		return new Supplier<String>() {
			@Override
			public String get() {
				return TypeMeta.get(obj, keyField, keyGetter);
			}
		};
	}

	private void findFields(Class<?> type) {
		List<Field> fields = new ArrayList<>();

		Class<?> superType = type;
		do {
			for(Field fld : superType.getDeclaredFields()) {
				fld.setAccessible(true);
				fields.add(fld);
			}
		} while(Object.class != (superType = type.getSuperclass()));

		for(Field fld : fields) {
			Annotation[] annos = fld.getDeclaredAnnotations();
			if(hasAnnotation(Metadata.class, annos)) {
				metadataField = fld;
			}
			if(hasAnnotation(Vclock.class, annos)) {
				vclockField = fld;
			}
			if(hasAnnotation(Key.class, annos)) {
				keyField = fld;
			}
		}
	}

	private void findMethods(Class<?> type) {
		List<Method> methods = new ArrayList<>();

		Class<?> superType = type;
		do {
			for(Method mth : superType.getDeclaredMethods()) {
				mth.setAccessible(true);
				methods.add(mth);
			}
		} while(Object.class != (superType = type.getSuperclass()));

		for(Method mth : methods) {
			Annotation[] annos = mth.getDeclaredAnnotations();
			boolean isGetter = mth.getParameterTypes().length == 0;
			if(hasAnnotation(Metadata.class, annos)) {
				if(isGetter) {
					metadataGetter = mth;
				} else {
					metadataSetter = mth;
				}
			}
			if(hasAnnotation(Vclock.class, annos)) {
				if(isGetter) {
					vclockGetter = mth;
				} else {
					vclockSetter = mth;
				}
			}
			if(hasAnnotation(Key.class, annos)) {
				if(isGetter) {
					keyGetter = mth;
				} else {
					keySetter = mth;
				}
			}
		}
	}

	private static void set(Object obj, Object arg, Field fld, Method mth) {
		try {
			if(null != mth) {
				mth.invoke(obj, arg);
			} else if(null != fld) {
				fld.set(obj, arg);
			}
		} catch(Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> T get(Object obj, Field fld, Method mth) {
		try {
			if(null != mth) {
				return (T)mth.invoke(obj);
			} else if(null != fld) {
				return (T)fld.get(obj);
			} else {
				return null;
			}
		} catch(Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static boolean hasAnnotation(Class<? extends Annotation> annoType, Annotation[] annos) {
		for(Annotation anno : annos) {
			if(anno.annotationType().isAssignableFrom(annoType)) {
				return true;
			}
		}
		return false;
	}

}
