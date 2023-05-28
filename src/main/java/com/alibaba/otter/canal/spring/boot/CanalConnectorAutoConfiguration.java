package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.impl.ClusterNodeAccessStrategy;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.client.impl.SimpleNodeAccessStrategy;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class, ClusterCanalConnector.class })
@ConditionalOnProperty(prefix = CanalConnectorProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties({CanalProperties.class, CanalConnectorProperties.class})
@Slf4j
public class CanalConnectorAutoConfiguration {

    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    public CanalConnector canalConnector(CanalConnectorProperties connectorProperties){

        if (StringUtils.hasText(connectorProperties.getZkServers())) {
            ClusterCanalConnector canalConnector = new ClusterCanalConnector(connectorProperties.getUsername(),
                    connectorProperties.getPassword(),
                    connectorProperties.getDestination(),
                    new ClusterNodeAccessStrategy(connectorProperties.getDestination(),
                            ZkClientx.getZkClient(connectorProperties.getZkServers())));
            canalConnector.setSoTimeout(connectorProperties.getSoTimeout());
            canalConnector.setIdleTimeout(connectorProperties.getIdleTimeout());
            canalConnector.setRetryTimes(connectorProperties.getRetryTimes());
            canalConnector.setRetryInterval(connectorProperties.getRetryInterval());
            return canalConnector;
        } else if (StringUtils.hasText(connectorProperties.getAddresses())) {
            ClusterCanalConnector canalConnector = new ClusterCanalConnector(
                    connectorProperties.getUsername(),
                    connectorProperties.getPassword(),
                    connectorProperties.getDestination(),
                    new SimpleNodeAccessStrategy(parseAddresses(connectorProperties.getAddresses())));
            canalConnector.setSoTimeout(connectorProperties.getSoTimeout());
            canalConnector.setIdleTimeout(connectorProperties.getIdleTimeout());
            canalConnector.setRetryTimes(connectorProperties.getRetryTimes());
            canalConnector.setRetryInterval(connectorProperties.getRetryInterval());
            return canalConnector;
        } else {

            InetSocketAddress address = new InetSocketAddress(connectorProperties.getHost(), connectorProperties.getPort());
            SimpleCanalConnector canalConnector = new SimpleCanalConnector(address,
                    connectorProperties.getDestination(),
                    connectorProperties.getUsername(),
                    connectorProperties.getPassword());
            canalConnector.setSoTimeout(connectorProperties.getSoTimeout());
            canalConnector.setIdleTimeout(connectorProperties.getIdleTimeout());
            return canalConnector;
        }
    }
/*

    public CanalConnectorConsumer canalConnectorConsumer(
            CanalProperties canalProperties,
            ObjectProvider<RocketMQCanalConnector> rocketMQCanalConnectorProvider,
            ObjectProvider<MessageListener> messageListenerProvider,
            ObjectProvider<FlatMessageListener> flatMessageListenerProvider){
        new CanalMQConnectorConsumer();
    }
*/

    private List<InetSocketAddress> parseAddresses(String addresses) {
        List<InetSocketAddress> parsedAddresses = new ArrayList<>();
        for (String address : StringUtils.commaDelimitedListToStringArray(addresses)) {
            if (StringUtils.hasText(address)) {
                String[] split = StringUtils.split(address, ":");
                Integer port = split.length == 1 ? CanalConnectorProperties.DEFAULT_PORT : Integer.parseInt(split[1]);
                parsedAddresses.add(new InetSocketAddress(split[0], port));
            }
        }
        return parsedAddresses;
    }
	
}
