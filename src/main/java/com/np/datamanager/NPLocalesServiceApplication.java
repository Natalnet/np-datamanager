package com.np.datamanager;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.np.commons.net.Http;
import com.np.commons.utils.Utils;

@SpringBootApplication
public class NPLocalesServiceApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(NPLocalesServiceApplication.class, args);
	}
	
	@Override
	public void run(String... args) throws Exception {
		Http.getInstance(10);
		Utils.getInstance();
	}
}
