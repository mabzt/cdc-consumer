package com.maison.mabs.cdcconsumer.s3.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "audit.s3")
public class AuditorConfigProperties {

	private String bucketName;

	private String prefix;

	private int flushSize;

	private String flushInterval;

}
