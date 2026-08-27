package com.maison.mabs.cdcconsumer.s3.domain.service;

import com.maison.mabs.cdcconsumer.audit.avro.AuditRecord;

public interface AuditS3Writer {

	void scheduleFlush();

	void write(AuditRecord auditRecord);

}
