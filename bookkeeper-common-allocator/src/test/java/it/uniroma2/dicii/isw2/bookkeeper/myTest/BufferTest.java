package it.uniroma2.dicii.isw2.bookkeeper.myTest;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

@RunWith(value=Parameterized.class)
public class BufferTest {
	
    int initialCapacity;
    int maxCapacity;
    ByteBufAllocator allocator;
    ByteBuf buffer;
    
    @Parameters
   	public static Collection<Integer[]> getTestParameters() {
   		return Arrays.asList(new Integer[][] {
   			{0, Integer.MAX_VALUE},
   			{512, Integer.MAX_VALUE},
   			{512, 512}
   			//{Integer.MAX_VALUE, Integer.MAX_VALUE}
   			//{Integer.MAX_VALUE, 512}
   			//{-512, Integer.MAX_VALUE}
   		});
   	}
   	
   	public BufferTest(int initialCapacity, int maxCapacity) {
   		this.configure(initialCapacity, maxCapacity);
   	}
   	
   	
   	public void configure(int initialCapacity, int maxCapacity) {
   		
   		this.initialCapacity = initialCapacity;
   		this.maxCapacity = maxCapacity;
   		
   		allocator = ByteBufAllocatorBuilder.create()
    			.pooledAllocator(PooledByteBufAllocator.DEFAULT)
    			.unpooledAllocator(null)
    			.poolingPolicy(PoolingPolicy.PooledDirect)
    			.leakDetectionPolicy(LeakDetectionPolicy.Disabled)
    			.build();
   	}
   	
    
    @Test
    public void testBuffer() {
    	
    	buffer = allocator.buffer(initialCapacity, maxCapacity);
    	assertEquals(maxCapacity, buffer.maxCapacity());
        buffer.release();
    }
}
