package com.vlink.iad;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/*
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.env.Environment;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.IOException;
import com.vlink.iad.service.AuthService;
import com.vlink.iad.service.AzureBlobService;
import com.vlink.iad.service.AzureVaultService;
import com.vlink.iad.service.PgpService;
*/

@SpringBootApplication
public class iadApplication {
    public static void main(String[] args) {
        SpringApplication.run(iadApplication.class, args);
    }
}
