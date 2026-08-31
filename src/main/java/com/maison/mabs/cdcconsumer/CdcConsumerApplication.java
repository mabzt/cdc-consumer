package com.maison.mabs.cdcconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class CdcConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(CdcConsumerApplication.class, args);
	}

}
