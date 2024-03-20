package com.alibaba.otter.canal.client.disruptor;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.disruptor.factory.CanalMessageFactory;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DisruptorCanalConnector extends SimpleCanalConnector implements InitializingBean, DisposableBean {

    private Disruptor<MessageEvent> disruptor;

    public DisruptorCanalConnector(SocketAddress address, String username, String password, String destination) {
        super(address, username, password, destination);
    }

    public DisruptorCanalConnector(SocketAddress address, String username, String password, String destination, int soTimeout) {
        super(address, username, password, destination, soTimeout);
    }

    public DisruptorCanalConnector(SocketAddress address, String username, String password, String destination, int soTimeout, int idleTimeout) {
        super(address, username, password, destination, soTimeout, idleTimeout);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        /**
         * 1、事件对象工厂
         */
        EventFactory<MessageEvent> eventFactory = CanalMessageFactory.INSTANCE;

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

    }

    @Override
    public List<Message> getList(Long timeout, TimeUnit unit) throws CanalClientException {
        return null;
    }

    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        return null;
    }

    @Override
    public List<FlatMessage> getFlatList(Long timeout, TimeUnit unit) throws CanalClientException {
        return null;
    }

    @Override
    public List<FlatMessage> getFlatListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        return null;
    }

    @Override
    public void ack() throws CanalClientException {

    }

    @Override
    public void connect() throws CanalClientException {

    }

    @Override
    public void disconnect() throws CanalClientException {

    }

    @Override
    public boolean checkValid() throws CanalClientException {
        return false;
    }

    @Override
    public void subscribe(String filter) throws CanalClientException {

    }

    @Override
    public void subscribe() throws CanalClientException {

    }

    @Override
    public void unsubscribe() throws CanalClientException {

    }

    @Override
    public Message get(int batchSize) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void rollback() throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void destroy() throws Exception {
        if(Objects.nonNull(disruptor)){
            disruptor.shutdown();
        }
    }
}
