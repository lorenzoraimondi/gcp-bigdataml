package gcp.cm.bigdata.adtech;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

// @SpringBootApplication is the same as @Configuration, @EnableAutoConfiguration and @ComponentScan
@SpringBootApplication
public class HttpApiMain {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(HttpApiMain.class.getResourceAsStream("/gcp.properties"));
        PubsubHelper.setConfig(props);

        SpringApplication.run(HttpApiMain.class, args);
    }

}
