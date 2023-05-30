package com.alibaba.otter.canal.spring.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CanalDisruptorApplication_Test {

    @Bean
    public CanalMessageEventHandler canalMessageEventHandler(){
        return new CanalMessageEventHandler();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(CanalDisruptorApplication_Test.class, args);
    }

}
