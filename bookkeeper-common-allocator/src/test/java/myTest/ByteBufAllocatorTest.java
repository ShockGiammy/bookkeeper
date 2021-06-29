package myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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

public class ByteBufAllocatorTest {

    ByteBufAllocator allocator;
    ByteBufAllocator allocator2;
    ByteBuf buffer;
    ByteBuf buffer2;
    private static final int DEFAULT_INITIAL_CAPACITY = 256;
    private static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;
    ByteBufAllocator mockAllocator;
    OutOfMemoryError outOfMemException;
    ByteBufAllocator heapAlloc;
    OutOfMemoryError noHeapException;
    AtomicReference<OutOfMemoryError> receivedException;
    
    @Before
    public void configure() {
    	mockAllocator = mock(ByteBufAllocator.class);
    	outOfMemException = new OutOfMemoryError("no mem");
    	
        heapAlloc = mock(ByteBufAllocator.class);
        noHeapException = new OutOfMemoryError("no more heap");
        
        receivedException = new AtomicReference<>();
    }

    @Test
    public void testEmptyByteBufAllocator() {
    	
    	allocator = ByteBufAllocatorBuilder.create().build();   	
    	assertTrue(allocator.isDirectBufferPooled());
    	
    	buffer = allocator.buffer();
    	assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
    	buffer.release();
    }
    
    @Test
    public void testUnpooled() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)
                .build();

        buffer = allocator.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertTrue(buffer.hasArray());
        
        assertFalse(allocator.isDirectBufferPooled());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testNullUnpooled() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)		//same as null
                .unpooledAllocator(null)				//same as "UnpooledByteBufAllocator.DEFAULT"
                .build();

        buffer = allocator.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }

    @Test
    public void testNullEqualsDefault() {
    	
    	allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)		//same as null
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)				//same as "UnpooledByteBufAllocator.DEFAULT"
                .build();
    	
    	allocator2 = ByteBufAllocatorBuilder.create()
                .poolingPolicy(null)		//same as null
                .unpooledAllocator(null)				//same as "UnpooledByteBufAllocator.DEFAULT"
                .build();
    	
    	buffer = allocator.buffer();
    	buffer2 = allocator.buffer();
    	
    	assertEquals(buffer.alloc(), buffer2.alloc());
    	
    	buffer.release();
    	buffer2.release();
    }
    @Test
    public void testDefaultPooled() {

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .pooledAllocator(PooledByteBufAllocator.DEFAULT)
                .unpooledAllocator(null)
                .build();

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertEquals(PooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertFalse(buffer.hasArray());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testPooledWithUnpooledHeap() {

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .pooledAllocator(PooledByteBufAllocator.DEFAULT)
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)
                .build();

        assertFalse(allocator.isDirectBufferPooled());			//il polled allocator viene non considerato
        														//anche pooling concurrency non viene considerato

        buffer = allocator.buffer();
        
        assertNotEquals(PooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertTrue(buffer.hasArray());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }

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
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testPooledWithConcurrencyZero() {				//correctly work
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .poolingConcurrency(0)
                .unpooledAllocator(null)
                .build();

        assertFalse(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertFalse(buffer.hasArray());
        System.out.println(((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(0, ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testPooledWithConcurrencyUno() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .poolingConcurrency(1)
                .unpooledAllocator(null)
                .build();

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertFalse(buffer.hasArray());
        assertEquals(1, ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testPooledWithConcurrencyThree() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .poolingConcurrency(3)
                .unpooledAllocator(null)
                .build();

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertFalse(buffer.hasArray());
        assertEquals(3, ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }

    @Test
    public void testPooledWithConcurrencyHigh() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .poolingConcurrency(30)
                .unpooledAllocator(null)
                .build();

        assertTrue(allocator.isDirectBufferPooled());

        buffer = allocator.buffer();
        assertFalse(buffer.hasArray());
        assertEquals(30, ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    // viewing implementation
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
        assertEquals(8, ((PooledByteBufAllocator) buffer.alloc()).metric().numDirectArenas());
        assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }

    
    @Test
    public void testOutOfMemoryPolicyWithException() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemException);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Disabled.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(outOfMemException, e);
        }
        assertEquals(outOfMemException.getMessage(), receivedException.get().getMessage());
    }
    
    @Test
    public void testOutOfMemoryPolicyWithExceptionSimple() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemException);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .leakDetectionPolicy(LeakDetectionPolicy.Simple)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Simple.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(outOfMemException, e);
        }
        assertEquals(outOfMemException.getMessage(), receivedException.get().getMessage());
    }
    
    @Test
    public void testOutOfMemoryPolicyWithExceptionAvanced() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemException);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .leakDetectionPolicy(LeakDetectionPolicy.Advanced)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Advanced.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(outOfMemException, e);
        }
        assertEquals(outOfMemException.getMessage(), receivedException.get().getMessage());
    }
    
    @Test
    public void testOutOfMemoryPolicyWithExceptionParanoid() {
        
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemException);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .leakDetectionPolicy(LeakDetectionPolicy.Paranoid)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();
        assertEquals(LeakDetectionPolicy.Paranoid.toString().toUpperCase(), ResourceLeakDetector.getLevel().toString());

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(outOfMemException, e);
        }
        assertEquals(outOfMemException.getMessage(), receivedException.get().getMessage());
    }

    @Test
    public void testOutOfMemoryPolicyWithFallback() {
    
        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemException);

        allocator = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        // Should not throw exception
        buffer = allocator.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());

        // No notification should have been triggered
        assertEquals(null, receivedException.get());
    }
    
    //fino qui

    @Test
    public void testOutOfMemoryPolicyWithFallbackAndNoMoreHeap() {

        when(mockAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemException);
        when(heapAlloc.heapBuffer(anyInt(), anyInt())).thenThrow(noHeapException);

        allocator = ByteBufAllocatorBuilder.create()
                .pooledAllocator(mockAllocator)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
        	allocator.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(noHeapException, e);
        }
        assertEquals(noHeapException.getMessage(), receivedException.get().getMessage());
    }

    @Test
    public void testOutOfMemoryPolicyUnpooledDirect() {
    
        ByteBufAllocator heapAlloc = mock(ByteBufAllocator.class);
        OutOfMemoryError noMemError = new OutOfMemoryError("no more direct mem");
        when(heapAlloc.directBuffer(anyInt(), anyInt())).thenThrow(noMemError);

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
        	allocator.directBuffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(noMemError, e);
        }
        assertEquals(noMemError.getMessage(), receivedException.get().getMessage());
    }

    @Test
    public void testOutOfMemoryPolicyUnpooledWithHeap() {
    
        when(heapAlloc.heapBuffer(anyInt(), anyInt())).thenThrow(noHeapException);

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
        	allocator.heapBuffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(noHeapException, e);
        }
        assertEquals(noHeapException.getMessage(), receivedException.get().getMessage());
    }
    
    @Test
    public void testNoListener() {
    
        when(heapAlloc.buffer(anyInt(), anyInt())).thenThrow(noHeapException);

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .pooledAllocator(mockAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .outOfMemoryListener(null)
                .build();

        allocator.buffer();
        
        assertNull(receivedException.get());
    }
    
    //test for buffer
    @Test
    public void testBufferCorrectlyValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
    	assertEquals(DEFAULT_MAX_CAPACITY, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testBufferSmallerValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(50, 100);
    	assertEquals(100, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testBufferWrongValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(100, 100);
    	assertEquals(100, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues2() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(100, 50);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues3() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(-100, 50);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues4() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(-50, -100);
    }
    
    @Test(expected=OutOfMemoryError.class)
    public void testBufferWrongValues5() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(DEFAULT_MAX_CAPACITY, DEFAULT_MAX_CAPACITY);
    }
}
