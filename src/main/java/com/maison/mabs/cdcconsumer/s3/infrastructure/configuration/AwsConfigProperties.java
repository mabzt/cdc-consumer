package com.maison.mabs.cdcconsumer.s3.infrastructure.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Setter
@Getter
@Configuration
@ConfigurationProperties("aws.s3")
public class AwsConfigProperties {

	private String accessKey;

	private String secretKey;

	private String region;

	private String endpoint;

}
