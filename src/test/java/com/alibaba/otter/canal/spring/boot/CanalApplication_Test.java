package com.alibaba.otter.canal.spring.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CanalApplication_Test {

    @Bean
    public CanalMessageEntryHandler canalMessageListener(){
        return new CanalMessageEntryHandler();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(CanalApplication_Test.class, args);
    }

}
