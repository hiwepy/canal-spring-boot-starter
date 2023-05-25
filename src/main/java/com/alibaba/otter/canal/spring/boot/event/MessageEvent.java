package com.alibaba.otter.canal.spring.boot.event;

import lombok.Data;

@Data
public class MessageEvent<T> {

    String uuid;

    T message;

}
