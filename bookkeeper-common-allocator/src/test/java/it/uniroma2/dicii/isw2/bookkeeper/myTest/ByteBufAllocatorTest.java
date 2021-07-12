package it.uniroma2.dicii.isw2.bookkeeper.myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value=Parameterized.class)
public class ByteBufAllocatorTest {

	ByteBufAllocator pooledAllocator;
	ByteBufAllocator unpooledAllocator;
	PoolingPolicy poolingPolicy;
    int poolingConcurrency;
    OutOfMemoryPolicy outOfMemoryPolicy;
    Consumer<OutOfMemoryError> outOfMemoryListener;
    LeakDetectionPolicy leakDetectionPolicy;
    int initialCapacity;
    int maxCapacity;
    
    ByteBufAllocator allocator;
    ByteBuf buffer;
    
    @Parameters
	public static Collection<Object[]> getTestParameters() {
		return Arrays.asList(new Object[][] {
			{null, null, null, 8, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, UnpooledByteBufAllocator.DEFAULT, PoolingPolicy.UnpooledHeap, 8, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, null, PoolingPolicy.UnpooledHeap, 8, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, UnpooledByteBufAllocator.DEFAULT, PoolingPolicy.UnpooledHeap, 8, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{PooledByteBufAllocator.DEFAULT, null, PoolingPolicy.PooledDirect, 4, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, null, PoolingPolicy.UnpooledHeap, 8, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{PooledByteBufAllocator.DEFAULT, UnpooledByteBufAllocator.DEFAULT, null, 8, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			//{null, null, PoolingPolicy.PooledDirect, -1, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			//{null, null, PoolingPolicy.PooledDirect, 0, null, null, null, 256, Integer.MAX_VALUE},
			{null, null, PoolingPolicy.PooledDirect, 0, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, null, PoolingPolicy.PooledDirect, 1, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, null, PoolingPolicy.PooledDirect, 3, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE},
			{null, null, PoolingPolicy.PooledDirect, 30, null, null, LeakDetectionPolicy.Disabled, 256, Integer.MAX_VALUE}
		});
	}
    
	public ByteBufAllocatorTest(ByteBufAllocator pooledAllocator, ByteBufAllocator unpooledAllocator, PoolingPolicy poolingPolicy,
		    int poolingConcurrency, OutOfMemoryPolicy outOfMemoryPolicy, Consumer<OutOfMemoryError> outOfMemoryListener,
		    LeakDetectionPolicy leakDetectionPolicy, int initialCapacity, int maxCapacity) {
		this.configure(pooledAllocator, unpooledAllocator, poolingPolicy, poolingConcurrency, outOfMemoryPolicy, outOfMemoryListener, leakDetectionPolicy, initialCapacity, maxCapacity);
	}
	
	
    public void configure(ByteBufAllocator pooledAllocator, ByteBufAllocator unpooledAllocator, PoolingPolicy poolingPolicy,
    int poolingConcurrency, OutOfMemoryPolicy outOfMemoryPolicy, Consumer<OutOfMemoryError> outOfMemoryListener,
    LeakDetectionPolicy leakDetectionPolicy, int initialCapacity, int maxCapacity) {
    	this.allocator = pooledAllocator;
    	this.unpooledAllocator= unpooledAllocator;
    	this.poolingPolicy = poolingPolicy;
    	this.poolingConcurrency = poolingConcurrency;
    	this.outOfMemoryPolicy = outOfMemoryPolicy;
    	this.outOfMemoryListener = outOfMemoryListener;
    	this.leakDetectionPolicy = leakDetectionPolicy;
    	this.initialCapacity = initialCapacity;
    	this.maxCapacity = maxCapacity;

    	allocator = ByteBufAllocatorBuilder.create()
    			.pooledAllocator(pooledAllocator)
    			.unpooledAllocator(unpooledAllocator)
    			.poolingPolicy(poolingPolicy)
    			.poolingConcurrency(poolingConcurrency)
    			.outOfMemoryPolicy(outOfMemoryPolicy)
    			.outOfMemoryListener(outOfMemoryListener)
    			.leakDetectionPolicy(leakDetectionPolicy)
    			.build();
    }
    
    @Test
    public void testByteBufAllocatorImpl() {
    

    	buffer = allocator.buffer();
    	assertEquals(maxCapacity, buffer.maxCapacity());
    	assertEquals(leakDetectionPolicy.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

    	if (poolingPolicy == PoolingPolicy.PooledDirect) {
    		assertFalse(buffer.hasArray());
    		if (poolingConcurrency > 0) 
    			assertTrue(allocator.isDirectBufferPooled());
    		else if (poolingConcurrency == 0) 
    			assertFalse(allocator.isDirectBufferPooled());
    		if (pooledAllocator == PooledByteBufAllocator.DEFAULT) {
    			assertEquals(PooledByteBufAllocator.DEFAULT, buffer.alloc());
    			assertEquals(2 * Runtime.getRuntime().availableProcessors(), ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
    		}
    	}
    	else {
    		assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());
    		assertTrue(buffer.hasArray());
    		assertFalse(allocator.isDirectBufferPooled());
    	}
    	buffer.release();
    }
}