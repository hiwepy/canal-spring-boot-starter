package com.alibaba.otter.canal.spring.boot.hooks;

import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.Objects;

public class DisruptorShutdownHook extends Thread {
	
	private Disruptor<MessageEvent> disruptor;
	
	public DisruptorShutdownHook(Disruptor<MessageEvent> disruptor) {
		this.setName("canal-disruptor-shutdown-hook");
		this.disruptor = disruptor;
	}
	
	@Override
	public void run() {
		if(Objects.nonNull(disruptor)){
			disruptor.shutdown();
		} 
	}
	
}
