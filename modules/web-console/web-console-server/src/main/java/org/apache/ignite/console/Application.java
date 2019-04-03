package org.apache.ignite.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

/** */
@SpringBootApplication
public class Application {
    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
