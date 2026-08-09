package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.util.AddressUtils;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AddressUtilsTest {

    @Test
    void parseAddressesReturnsList() {
        List<InetSocketAddress> addresses = AddressUtils.parseAddresses("127.0.0.1:11111,192.168.1.1:11111");
        assertThat(addresses).hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    void parseAddressesSingleAddress() {
        List<InetSocketAddress> addresses = AddressUtils.parseAddresses("localhost:11111");
        assertThat(addresses).hasSize(1);
    }

    @Test
    void parseAddressesEmptyString() {
        List<InetSocketAddress> addresses = AddressUtils.parseAddresses("");
        assertThat(addresses).isEmpty();
    }

    @Test
    void parseAddressesMultipleWithMixedPorts() {
        List<InetSocketAddress> addresses = AddressUtils.parseAddresses("host1:22222,host3:33333");
        assertThat(addresses).hasSize(2);
        assertThat(addresses.get(0).getPort()).isEqualTo(22222);
        assertThat(addresses.get(1).getPort()).isEqualTo(33333);
    }
}
