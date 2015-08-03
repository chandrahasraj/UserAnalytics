package com.peel.kinesisStorm;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

public class ConfigurationParameters {

	private static CircularFifoBuffer showAndviewersQueue ;
	private static CircularFifoBuffer uniqueViewersQueue ;
	private static CircularFifoBuffer uniqueTemporaryBuffer ;
	private static long windowSize ;
	private static String serviceAddress;

	private static StormConfigurator store;
	
	ConfigurationParameters(StormConfigurator config){
		store=config;
	}
	
	
	
	public static String getServiceAddress() {
		if(serviceAddress==null)
			serviceAddress=store.SERVICE_ADDRESS;
		return serviceAddress;
	}

	public static long getWindowSize() {
		if(String.valueOf(windowSize)==null||windowSize==0l)
			windowSize=store.WINDOW_FRAME_SIZE;
		return windowSize;
	}

	public static CircularFifoBuffer getUniqueTemporaryBuffer() {
		if(uniqueTemporaryBuffer==null)
			uniqueTemporaryBuffer=new CircularFifoBuffer(store.NUMBER_OF_FRAMES);
		return uniqueTemporaryBuffer;
	}

	public static CircularFifoBuffer getShowandviewersqueue() {
		if(showAndviewersQueue==null)
			showAndviewersQueue =new CircularFifoBuffer(store.NUMBER_OF_FRAMES);
		return showAndviewersQueue;
	}

	public static CircularFifoBuffer getUniqueviewersqueue() {
		if(uniqueViewersQueue==null)
			uniqueViewersQueue =new CircularFifoBuffer(store.NUMBER_OF_FRAMES);
		return uniqueViewersQueue;
	}
	
	
}
