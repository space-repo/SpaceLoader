package com.tmobile.space.loader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource({"classpath:cassandra-config.xml"})
public class TmoSpaceLoaderV2Application {

	public static void main(String[] args) {
		SpringApplication.run(TmoSpaceLoaderV2Application.class, args);
	}
}
