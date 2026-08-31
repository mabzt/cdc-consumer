package com.maison.mabs.cdcconsumer.kafka.infrastracture.configuration.utils;

import lombok.experimental.UtilityClass;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.Optional;

@UtilityClass
public class GenericRecordUtil {

	@SuppressWarnings({ "unchecked", "SpringHideUtilityClassConstructor" })
	public <T> Optional<T> getValue(GenericRecord record, String fieldName) {
		if (record == null) {
			return Optional.empty();
		}

		Object value = record.get(fieldName);
		if (value == null) {
			return Optional.empty();
		}

		// Avro stores string as Utf8, not java.lang.String
		if (value instanceof Utf8) {
			return Optional.of((T) value.toString());
		}

		return Optional.of((T) value);
	}

	public Optional<String> getString(GenericRecord record, String fieldName) {
		return getValue(record, fieldName);
	}

	public Optional<Long> getLong(GenericRecord record, String fieldName) {
		return getValue(record, fieldName);
	}

	public Optional<Integer> getInt(GenericRecord record, String fieldName) {
		return getValue(record, fieldName);
	}

	public Optional<Boolean> getBoolean(GenericRecord record, String fieldName) {
		return getValue(record, fieldName);
	}

	public Optional<GenericRecord> getNested(GenericRecord record, String fieldName) {
		return getValue(record, fieldName);
	}

}
