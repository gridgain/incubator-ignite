package org.apache.ignite.console;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** */
@SpringBootApplication
public class Application {
    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    /**
     *  TODO IGNITE-5617 javadocs.
     *
     *  Do we really need this bean here? not in some configuration?
     */
    @Bean
    public IgniteEx authProvider() {
        return (IgniteEx)Ignition.start(new IgniteConfiguration().setPeerClassLoadingEnabled(true));
    }
}
