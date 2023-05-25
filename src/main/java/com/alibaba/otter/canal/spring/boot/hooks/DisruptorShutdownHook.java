package com.alibaba.otter.canal.spring.boot.hooks;

import com.alibaba.otter.canal.spring.boot.disruptor.CanalDisruptorConsumer;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.List;
import java.util.Objects;

public class DisruptorShutdownHook extends Thread {
	
	private Disruptor<MessageEvent> disruptor;
	private List<CanalDisruptorConsumer> disruptorConsumers;
	
	public DisruptorShutdownHook(Disruptor<MessageEvent> disruptor, List<CanalDisruptorConsumer> disruptorConsumers) {
		this.setName("canal-disruptor-shutdown-hook");
		this.disruptor = disruptor;
		this.disruptorConsumers = disruptorConsumers;
	}
	
	@Override
	public void run() {
		if(Objects.nonNull(disruptor)){
			disruptor.shutdown();
		}
		if(Objects.nonNull(disruptorConsumers)){
			for (CanalDisruptorConsumer consumer: disruptorConsumers ) {
				consumer.stop();
			}
		}
	}
	
}
