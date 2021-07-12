package it.uniroma2.dicii.isw2.bookkeeper.myTest;

import static org.junit.Assert.assertEquals;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

public class BufferExceptionsTest {
	
    int initialCapacity;
    int maxCapacity;
    ByteBufAllocator allocator;
    ByteBuf buffer;
    int wrongInitialCapacity;
	
	@Before
	public void configure() {
		
		maxCapacity = Integer.MAX_VALUE;
		initialCapacity = 512;
		wrongInitialCapacity = -512;
	}
	
	@Test(expected=OutOfMemoryError.class)
    public void testBufferWrongValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(maxCapacity, maxCapacity);
    	assertEquals(maxCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues2() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(maxCapacity, initialCapacity);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues3() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(wrongInitialCapacity, maxCapacity);
    }
}
