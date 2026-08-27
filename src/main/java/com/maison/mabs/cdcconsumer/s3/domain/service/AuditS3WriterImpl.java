package com.maison.mabs.cdcconsumer.s3.domain.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maison.mabs.cdcconsumer.audit.avro.AuditRecord;
import com.maison.mabs.cdcconsumer.s3.infrastructure.AuditorConfigProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuditS3WriterImpl implements AuditS3Writer {

	private static final String FORMAT = "%s/table_name=%s/year=%s/month=%s/day=%s";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	// private final List<AuditRecord> auditRecords = new ArrayList<>();

	private final List<AuditRecord> buffer = Collections.synchronizedList(new ArrayList<>());

	private final Object lock = new Object();

	private final AuditorConfigProperties configProperties;

	private final S3Client s3Client;

	@Override
	@Scheduled(cron = "0 */5 * * * *")
	// @Scheduled(cron = "${audit.s3.flush-interval-ms}")
	public void scheduleFlush() {
		log.info("Scheduled job started at : {}", LocalDateTime.now());
		synchronized (this.lock) {
			if (!this.buffer.isEmpty()) {
				flush();
			}
		}
		log.info("Scheduled job finished at : {}", LocalDateTime.now());
	}

	@Override
	public void write(AuditRecord auditRecord) {
		// The lock object exists because this class is accessed from two different
		// threads simultaneously
		// Without synchronization both threads could be modifying buffer at the same
		// time, resulting in
		// ConcurrentModificationException or lost records.
		synchronized (this.lock) {
			this.buffer.add(auditRecord);
			if (this.buffer.size() >= this.configProperties.getFlushSize()) {
				flush();
			}
		}
	}

	private void flush() {
		List<AuditRecord> batch = new ArrayList<>(this.buffer);
		this.buffer.clear();

		batch.stream()
			.collect(Collectors.groupingBy(r -> String.format(FORMAT, this.configProperties.getPrefix(),
					r.getTableName(), r.getYear(), r.getMonth(), r.getDay())))
			.forEach(this::writeParquet);
	}

	private void writeParquet(String partitionKey, List<AuditRecord> records) {
		String key = partitionKey + "/" + UUID.randomUUID() + ".parquet";

		try {
			byte[] parquetBytes = toParquetBytes(records);
			this.s3Client.putObject(PutObjectRequest.builder()
				.bucket(this.configProperties.getBucketName())
				.key(key)
				.contentType("application/octet-stream")
				.contentLength((long) parquetBytes.length)
				.build(), RequestBody.fromBytes(parquetBytes));

			log.info("Flushed {} audit records to s3://{}/{}", records.size(), this.configProperties.getBucketName(),
					key);
		}
		catch (IOException ex) {
			log.error("Failed to write Parquet file to S3: {}", key, ex);
			throw new RuntimeException("Parquet write failed", ex);
		}
	}

	private byte[] toParquetBytes(List<AuditRecord> records) throws IOException {
		ByteArrayOutputFile outputFile = new ByteArrayOutputFile();

		// ParquetWriter uses try-with-resources — close() finalises the file footer
		try (ParquetWriter<AuditRecord> writer = AvroParquetWriter.<AuditRecord>builder(outputFile)
			.withSchema(SCHEMA)
			.withCompressionCodec(CompressionCodecName.SNAPPY)
			.withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
			.withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
			.withDictionaryEncoding(true)
			.build()) {

			for (AuditRecord record : records) {
				writer.write(record);
			}
		}
		// close() must complete before reading bytes — the footer is written on close
		return outputFile.toByteArray();
	}

	private static final Schema SCHEMA = SchemaBuilder.record("AuditRecord")
		.namespace("com.maison.mabs.cdcconsumer.audit.avro")
		.fields()
		.requiredString("tableName")
		.requiredString("operation")
		.optionalString("before")
		.optionalString("after")
		.name("lsn")
		.type()
		.nullable()
		.longType()
		.noDefault()
		.requiredLong("occurredAt")
		.requiredString("year")
		.requiredString("month")
		.requiredString("day")
		.endRecord();

}
