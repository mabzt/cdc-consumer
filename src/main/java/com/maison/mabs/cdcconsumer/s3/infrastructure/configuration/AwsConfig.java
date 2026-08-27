package com.maison.mabs.cdcconsumer.s3.infrastructure.configuration;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Configuration
@RequiredArgsConstructor
public class AwsConfig {

	private final AwsConfigProperties configProperties;

	@Bean
	public S3Client s3Client() {
		return S3Client.builder()
			.endpointOverride(URI.create(this.configProperties.getEndpoint()))
			.region(Region.of(this.configProperties.getRegion()))
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials
				.create(this.configProperties.getAccessKey(), this.configProperties.getSecretKey())))
			.forcePathStyle(true) // Required for minio
			.build();
	}

}
