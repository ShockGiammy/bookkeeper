package it.uniroma2.dicii.isw2.bookkeeper.myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBufAllocator;

public class ByteBufAllocatorAddedTest {
	
	ByteBufAllocator allocator;
	OutOfMemoryError outOfMemError;
	ByteBufAllocator unpooledAllocator;
    AtomicReference<OutOfMemoryError> receivedError;
	
	@Before
	public void configure() {
		
		outOfMemError = new OutOfMemoryError("no mem");
		unpooledAllocator = mock(ByteBufAllocator.class);
		receivedError = new AtomicReference<>();
		
		when(unpooledAllocator.directBuffer(anyInt(), anyInt())).thenThrow(outOfMemError);
		
		
		allocator = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(unpooledAllocator)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedError.set(e);
                })
                .build();

	}

	@Test
    public void testOutOfMemoryPolicyUnpooledDirect() {

        try {
        	allocator.directBuffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            assertEquals(outOfMemError, e);
        }
        assertEquals(outOfMemError.getMessage(), receivedError.get().getMessage());
    }
}
