package gcp.cm.bigdata.adtech;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;

// @SpringBootApplication is the same as @Configuration, @EnableAutoConfiguration and @ComponentScan
@SpringBootApplication
public class HttpApiMain {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(HttpApiMain.class, args);
    }

}
