package myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
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
    ByteBufAllocator mockAllocator;
    OutOfMemoryError outOfMemError;
    ByteBufAllocator heapAlloc;
    OutOfMemoryError noHeapError;
    AtomicReference<OutOfMemoryError> receivedError;
    int initialCapacity;
    int defaultmaxCapacity;
    int wrongInitialCapacity;
    
    @Before
    public void configure() {
    	mockAllocator = mock(ByteBufAllocator.class);
    	outOfMemError = new OutOfMemoryError("no mem");
    	
        heapAlloc = mock(ByteBufAllocator.class);
        noHeapError = new OutOfMemoryError("no more heap");
        
        receivedError = new AtomicReference<>();
        
        initialCapacity = 256;
        defaultmaxCapacity = Integer.MAX_VALUE;
        wrongInitialCapacity = -256;
    }

    @Test
    public void testEmptyByteBufAllocator() {
    	
    	allocator = ByteBufAllocatorBuilder.create().build();   	
    	assertTrue(allocator.isDirectBufferPooled());
    	
    	buffer = allocator.buffer();
    	assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testNullUnpooled() {
    	
        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)		//same as null
                .unpooledAllocator(null)						//same as "UnpooledByteBufAllocator.DEFAULT"
                .build();

        buffer = allocator.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buffer.alloc());
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }

    @Test
    public void testNullEqualsDefault() {
    	
    	allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)
                .build();
    	
    	allocator2 = ByteBufAllocatorBuilder.create()
                .poolingPolicy(null)
                .unpooledAllocator(null)
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
        assertEquals(defaultmaxCapacity, buffer.maxCapacity());
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
    
    //fino qui

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
    
    //test for buffer
    @Test
    public void testBufferCorrectlyValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(initialCapacity, defaultmaxCapacity);
    	assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testBufferInital0() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(0, defaultmaxCapacity);
    	assertNotNull(buffer.maxCapacity());
        buffer.release();
    }
    
    @Test
    public void testBufferSameValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(initialCapacity, initialCapacity);
    	assertEquals(initialCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test(expected=OutOfMemoryError.class)
    public void testBufferWrongValues() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(defaultmaxCapacity, defaultmaxCapacity);
    	assertEquals(defaultmaxCapacity, buffer.maxCapacity());
        buffer.release();
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues2() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(defaultmaxCapacity, initialCapacity);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBufferWrongValues3() {
    	allocator = ByteBufAllocatorBuilder.create()
            .poolingPolicy(PoolingPolicy.PooledDirect)
            .pooledAllocator(PooledByteBufAllocator.DEFAULT)
            .unpooledAllocator(null)
            .build();
    	
    	buffer = allocator.buffer(wrongInitialCapacity, defaultmaxCapacity);
    }
    
    @Test			//to upgrade branch coverage (line 183)
    public void testOutOfMemoryPolicyUnpooledDirect() {
    
        when(heapAlloc.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);

        allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();

        try {
        	allocator.directBuffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(outOfMemError, e);
        }
        assertEquals(outOfMemError.getMessage(), receivedError.get().getMessage());
    }
}
