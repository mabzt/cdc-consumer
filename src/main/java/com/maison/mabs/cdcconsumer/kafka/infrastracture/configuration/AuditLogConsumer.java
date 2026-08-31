package com.maison.mabs.cdcconsumer.kafka.infrastracture.configuration;

import com.maison.mabs.cdcconsumer.audit.avro.AuditRecord;
import com.maison.mabs.cdcconsumer.kafka.exception.KafkaException;
import com.maison.mabs.cdcconsumer.kafka.infrastracture.configuration.utils.GenericRecordUtil;
import com.maison.mabs.cdcconsumer.s3.domain.service.AuditS3Writer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class AuditLogConsumer {

	private final AuditS3Writer auditS3Writer;

	@KafkaListener(topics = "cdc.public.users", groupId = "audit-log-group")
	public void consume(ConsumerRecord<GenericRecord, GenericRecord> record) {
		GenericRecord payload = record.value();
		if (Objects.isNull(payload)) {
			log.info("No payload found for key {}", record.key());
			return;
		}

		GenericRecord source = (GenericRecord) payload.get("source");
		long tsMs = GenericRecordUtil.getLong(payload, "ts_ms").orElseThrow(() -> new KafkaException("Missing ts_ms"));
		String operation = GenericRecordUtil.getString(payload, "op")
			.orElseThrow(() -> new KafkaException("Missing op field in CDC event"));
		Long logSequenceNumber = GenericRecordUtil.getLong(source, "lsn").orElse(null);

		String afterJson = GenericRecordUtil.getNested(payload, "after").map(Object::toString).orElse(null);

		String beforeJson = GenericRecordUtil.getNested(payload, "before").map(Object::toString).orElse(null);

		LocalDate date = Instant.ofEpochMilli(tsMs).atZone(ZoneOffset.UTC).toLocalDate();

		AuditRecord auditRecord = AuditRecord.newBuilder()
			.setTableName("users")
			.setOperation(operation)
			.setBefore(beforeJson)
			.setAfter(afterJson)
			.setLsn(logSequenceNumber)
			.setOccurredAt(tsMs)
			.setYear(String.valueOf(date.getYear()))
			.setMonth(String.valueOf(date.getMonth()))
			.setDay(String.valueOf(date.getDayOfMonth()))
			.build();

		log.info("Audit record queued — op={}, lsn={}", operation, logSequenceNumber);
		this.auditS3Writer.write(auditRecord);

	}

}
