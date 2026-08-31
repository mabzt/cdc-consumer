package com.maison.mabs.cdcconsumer.s3.domain.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AuditRecord {

	/**
	 * Internal representation of single CDC event.
	 */
	private String tableName;

	/**
	 * Maps to Debezium op, indicates which event happened. C : inser U : update D :
	 * delete R : snapshot read
	 */
	private String operation;

	private String before;

	private Long after;

	/**
	 * Log sequence number from Postgres WAL, represent offset change in the WAL stream.
	 *
	 */
	private Long lsn;

	private Long occurredAt;

	private String year;

	private String month;

	private String day;

}
