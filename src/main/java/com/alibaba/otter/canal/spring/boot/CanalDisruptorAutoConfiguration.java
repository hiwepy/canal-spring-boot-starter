package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.spring.boot.disruptor.CanalDisruptorConsumer;
import com.alibaba.otter.canal.spring.boot.disruptor.CanalDisruptorConsumers;
import com.alibaba.otter.canal.spring.boot.disruptor.CanalEventHandler;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.disruptor.factory.CanalEventFactory;
import com.alibaba.otter.canal.spring.boot.hooks.DisruptorShutdownHook;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConditionalOnClass({ CanalConnector.class, CanalLifeCycle.class, CanalPacket.class, Disruptor.class })
@ConditionalOnProperty(prefix = CanalDisruptorProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalDisruptorProperties.class)
@Slf4j
public class CanalDisruptorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CanalEventHandler canalEventHandler(){
        return new CanalEventHandler(){};
    }

    /**
     * 创建Disruptor
     * @param disruptorProperties	: 配置参数
     * @param canalConnectorProvider	    : CanalConnector 提供者
     * @param canalEventHandlerProvider	    : EventHandler 提供者
     * @return {@link Disruptor} instance
     */
    @Bean(destroyMethod = "shutdown", name = "canalDisruptor")
    public Disruptor<MessageEvent> canalDisruptor(
            CanalDisruptorProperties disruptorProperties,
            ObjectProvider<CanalConnector> canalConnectorProvider,
            ObjectProvider<CanalEventHandler> canalEventHandlerProvider) {

        /**
         * 1、事件对象工厂
         */
        EventFactory<MessageEvent> eventFactory = CanalEventFactory.INSTANCE;

        /**
         * 2、线程工厂
         */
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("canal-disruptor-%d")
                .daemon(true)
                .priority(Thread.MAX_PRIORITY)
                .build();
        /**
         * 3、初始化 Disruptor
         * http://blog.csdn.net/a314368439/article/details/72642653?utm_source=itdadao&utm_medium=referral
         */
        Disruptor<MessageEvent> disruptor = new Disruptor<>(eventFactory, disruptorProperties.getRingBufferSize(), threadFactory,
                disruptorProperties.getProducerType(), disruptorProperties.getWaitStrategy().get());

        /**
         * 4、绑定 Handler
         */
        disruptor.handleEventsWith(canalEventHandlerProvider.getIfAvailable());
        disruptor.handleExceptionsFor(canalEventHandlerProvider.getIfAvailable());

        disruptor.start();


        List<CanalDisruptorConsumer> disruptorConsumers = new ArrayList<>();
        canalConnectorProvider.stream().forEach(connector -> {

            CanalDisruptorConsumer consumer =  CanalDisruptorConsumers.create(connector, disruptor, disruptorProperties.getBatchSize(),
                    disruptorProperties.getTimeout(), disruptorProperties.getUnit());
            disruptorConsumers.add(consumer);
            consumer.start();

        });

        /**
         * 5、应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        Runtime.getRuntime().addShutdownHook(new DisruptorShutdownHook(disruptor, disruptorConsumers));

        return disruptor;

    }

}
