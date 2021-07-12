package it.uniroma2.dicii.isw2.bookkeeper.myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Before;
import org.junit.Test;

public class ByteBufAllocatorMockitoTest {

    ByteBufAllocator allocator;
    ByteBuf buffer;
    ByteBufAllocator mockAllocator;
    OutOfMemoryError outOfMemError;
    ByteBufAllocator heapAlloc;
    OutOfMemoryError noHeapError;
    AtomicReference<OutOfMemoryError> receivedError;
    int defaultmaxCapacity;
    
    @Before
    public void configure() {
    	mockAllocator = mock(ByteBufAllocator.class);
    	outOfMemError = new OutOfMemoryError("no mem");
    	
        heapAlloc = mock(ByteBufAllocator.class);
        noHeapError = new OutOfMemoryError("no more heap");
        
        receivedError = new AtomicReference<>();
        defaultmaxCapacity = Integer.MAX_VALUE;    }

    @Test(expected = IllegalArgumentException.class)
    public void testPooledWithConcurrencyNegative() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .poolingConcurrency(-1)
                .unpooledAllocator(null)
                .build();
        fail("IllegalArgument");

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertFalse(buffer.hasArray());
        assertEquals(-1, ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testOutOfMemoryPolicyWithException() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Disabled.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(outOfMemError, e);
        }
        assertEquals(outOfMemError.getMessage(), receivedError.get().getMessage());
    }
    
    @Test
    public void testOutOfMemoryPolicyWithExceptionSimple() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .leakDetectionPolicy(LeakDetectionPolicy.Simple)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Simple.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(outOfMemError, e);
        }
        assertEquals(outOfMemError.getMessage(), receivedError.get().getMessage());
    }
    
    @Test
    public void testOutOfMemoryPolicyWithExceptionAvanced() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .leakDetectionPolicy(LeakDetectionPolicy.Advanced)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Advanced.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(outOfMemError, e);
        }
        assertEquals(outOfMemError.getMessage(), receivedError.get().getMessage());
    }
    
    @Test
    public void testOutOfMemoryPolicyWithExceptionParanoid() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .leakDetectionPolicy(LeakDetectionPolicy.Paranoid)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Paranoid.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(outOfMemError, e);
        }
        assertEquals(outOfMemError.getMessage(), receivedError.get().getMessage());
    }

    @Test
    public void testOutOfMemoryPolicyWithFallback() {
    
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        allocator = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();

        // Should not throw exception
        buffer = allocator.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());

        // No notification should have been triggered
        assertEquals(null, receivedError.get());
    }

    @Test
    public void testOutOfMemoryPolicyWithFallbackAndNoMoreHeap() {

        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);
        when(heapAlloc.heapBuffer(anyInt(), anyInt())).thenThrow(noHeapError);

        allocator = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .unpooledAllocator(heapAlloc)
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();

        try {
        	allocator.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(noHeapError, e);
        }
        assertEquals(noHeapError.getMessage(), receivedError.get().getMessage());
    }

    @Test
    public void testOutOfMemoryPolicyUnpooledWithHeap() {
    
        when(heapAlloc.heapBuffer(anyInt(), anyInt())).thenThrow(noHeapError);

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();

        try {
        	allocator.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(noHeapError, e);
        }
        assertEquals(noHeapError.getMessage(), receivedError.get().getMessage());
    }
    
    @Test
    public void testNoListener() {
    
        when(heapAlloc.buffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .pooledAllocator(mockAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .outOfMemoryListener(null)
                .build();

        allocator.buffer();
        
        assertNull(receivedError.get());
    }
    
    @Test
    public void testDefaultPooled() {

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .pooledAllocator(null)
                .build();

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertEquals(PooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertFalse(buffer.hasArray());
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testPooledDefaultAllocatorWithConcurrency() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .pooledAllocator(PooledByteBufAllocator.DEFAULT)		//ignora poolingConcurrency
                .poolingConcurrency(2)	
                .unpooledAllocator(null)
                .build();

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertFalse(buffer.hasArray());
        assertEquals(2 * Runtime.getRuntime().availableProcessors(), ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }
}
