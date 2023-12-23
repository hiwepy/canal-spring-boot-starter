package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.spring.boot.consumer.impl.CanalConnectorDisruptorConsumerImpl;
import com.alibaba.otter.canal.spring.boot.consumer.impl.CanalMQDisruptorConnectorConsumerImpl;
import com.alibaba.otter.canal.spring.boot.disruptor.MessageEventHandler;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.disruptor.factory.CanalEventFactory;
import com.alibaba.otter.canal.spring.boot.hooks.DisruptorShutdownHook;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ CanalConnector.class, CanalLifeCycle.class, CanalPacket.class, Disruptor.class })
@ConditionalOnProperty(prefix = CanalProperties.PREFIX, value = "consumer-mode", havingValue = "DISRUPTOR")
@EnableConfigurationProperties({CanalProperties.class, CanalConsumerProperties.class, CanalDisruptorProperties.class})
@Slf4j
public class CanalDisruptorAutoConfiguration {


    @Bean
    @ConditionalOnMissingBean
    public MessageEventHandler canalEventHandler(){
        return new MessageEventHandler(){
            @Override
            public void onEvent(MessageEvent event) throws Exception {

            }
        };
    }

    /**
     * 创建Disruptor
     * @param disruptorProperties	: 配置参数
     * @param canalEventHandlerProvider	    : EventHandler 提供者
     * @return {@link Disruptor} instance
     */
    @Bean(initMethod = "start", destroyMethod = "shutdown", name = "canalDisruptor")
    public Disruptor<MessageEvent> canalDisruptor(
            CanalConsumerProperties consumerProperties,
            CanalDisruptorProperties disruptorProperties,
            ObjectProvider<MessageEventHandler> canalEventHandlerProvider) {

        /**
         * 1、事件对象工厂
         */
        EventFactory<MessageEvent> eventFactory = CanalEventFactory.INSTANCE;

        /**
         * 2、线程工厂
         */
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("canal-disruptor-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        /**ThreadFactory threadFactory = new ThreadFactoryImpl("canal-disruptor-");
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

        /**
         * 5、应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        Runtime.getRuntime().addShutdownHook(new DisruptorShutdownHook(disruptor));

        return disruptor;

    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public CanalConnectorDisruptorConsumerImpl canalConnectorDisruptorConsumer(
            CanalConsumerProperties consumerProperties,
            ObjectProvider<CanalConnector> canalConnectorProvider,
            @Qualifier("canalDisruptor") Disruptor<MessageEvent> canalDisruptor){

        List<CanalConnector> connectors = canalConnectorProvider.stream()
                .filter(connector -> !CanalMQConnector.class.isAssignableFrom(connector.getClass()))
                .collect(Collectors.toList());

        CanalConnectorDisruptorConsumerImpl consumerImpl = new CanalConnectorDisruptorConsumerImpl(connectors, canalDisruptor);
        consumerImpl.init(consumerProperties);

        return consumerImpl;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public CanalMQDisruptorConnectorConsumerImpl canalMQCanalConnectorDisruptorConsumer(
            CanalConsumerProperties consumerProperties,
            ObjectProvider<CanalMQConnector> rocketMQCanalConnectorProvider,
            @Qualifier("canalDisruptor") Disruptor<MessageEvent> canalDisruptor){

        List<CanalMQConnector> connectors = rocketMQCanalConnectorProvider.stream()
                .filter(connector -> CanalMQConnector.class.isAssignableFrom(connector.getClass()))
                .collect(Collectors.toList());

        CanalMQDisruptorConnectorConsumerImpl consumerImpl = new CanalMQDisruptorConnectorConsumerImpl(connectors, canalDisruptor);
        consumerImpl.init(consumerProperties);
        return consumerImpl;
    }

}
