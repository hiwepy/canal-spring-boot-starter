package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.spring.boot.disruptor.CanalEventHandler;
import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.event.factory.CanalEventFactory;
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

@Configuration
@ConditionalOnClass({ CanalConnector.class, CanalLifeCycle.class, CanalPacket.class })
@ConditionalOnProperty(prefix = CanalProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalProperties.class)
@Slf4j
public class CanalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public CanalEventHandler canalEventHandler(){
        return new CanalEventHandler(){};
    }

    /**
     * 创建Disruptor
     * @param properties	: 配置参数
     * @param canalEventHandler	: EventHandler 获取
     * @return {@link Disruptor} instance
     */
    @Bean(initMethod = "start", name = "canalDisruptor")
    @ConditionalOnClass({ Disruptor.class })
    public Disruptor<MessageEvent> canalDisruptor(
            CanalProperties properties,
            ObjectProvider<CanalEventHandler> canalEventHandler) {

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
        Disruptor<MessageEvent> disruptor = new Disruptor<>(eventFactory, properties.getDisruptor().getRingBufferSize(), threadFactory,
                properties.getDisruptor().getProducerType(), properties.getDisruptor().getWaitStrategy().get());

        /**
         * 4、绑定 Handler
         */
        disruptor.handleEventsWith(canalEventHandler.getIfAvailable());
        disruptor.handleExceptionsFor(canalEventHandler.getIfAvailable());

        /**
         * 5、应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        Runtime.getRuntime().addShutdownHook(new DisruptorShutdownHook(disruptor));

        return disruptor;

    }

}
