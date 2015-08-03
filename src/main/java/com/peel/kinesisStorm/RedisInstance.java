package com.peel.kinesisStorm;

import java.io.Serializable;

import redis.clients.jedis.Jedis;

public class RedisInstance implements Serializable {

	/**
	 * 
	 */

	private static final long serialVersionUID=1l;
	static Jedis jedis ;
	
	public static Jedis getInstance(){
		if(jedis==null){
			jedis= new Jedis("54.173.88.95");//54.173.88.95
		}
		return jedis;
	}
	
}

