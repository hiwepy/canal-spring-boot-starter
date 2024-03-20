package com.alibaba.otter.canal.spring.boot.utils;

import com.alibaba.otter.canal.spring.boot.CanalTcpProperties;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class AddressUtils {

    public static List<InetSocketAddress> parseAddresses(String addresses) {
        List<InetSocketAddress> parsedAddresses = new ArrayList<>();
        for (String address : StringUtils.commaDelimitedListToStringArray(addresses)) {
            if (StringUtils.hasText(address)) {
                String[] split = StringUtils.split(address, ":");
                Integer port = split.length == 1 ? CanalTcpProperties.DEFAULT_PORT : Integer.parseInt(split[1]);
                parsedAddresses.add(new InetSocketAddress(split[0], port));
            }
        }
        return parsedAddresses;
    }

}
