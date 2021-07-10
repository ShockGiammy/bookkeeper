package myTest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class BookieClientTest {
	
    BookieServer bs;
    File tmpDir;
    public int port;
    ServerConfiguration conf;

    public EventLoopGroup eventLoopGroup;
    public OrderedExecutor executor;
    private ScheduledExecutorService scheduler;
    private BookieClientImpl client;
    BookieId addr;
    BookieId wrongAddr;
    ResultStruct result;

    byte[] passwd;
    ByteBufList byteBuf;
    static int init_rc;
    int ledgerId;
    int entryId;

    @Before
    public void configure() throws Exception {
    	
    	//server configuation
    	port = 13645;
        tmpDir = IOUtils.createTempDir("bookieClient", "test");
        conf = new ServerConfiguration();
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setJournalFormatVersionToWrite(5);
        conf.setAllowEphemeralPorts(true);
        conf.setBookiePort(0);
        conf.setGcWaitTime(1000);
        conf.setDiskUsageThreshold(0.999f);
        conf.setDiskUsageWarnThreshold(0.99f);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        conf.setListeningInterface(getLoopbackInterfaceName());
        conf.setAllowLoopback(true);
        conf.setBookiePort(port).setJournalDirName(tmpDir.getPath()).setLedgerDirNames(new String[] { tmpDir.getPath() }).setMetadataServiceUri(null);

        //server start
        bs = new BookieServer(conf);
        bs.start();
        
        
        //client configuration
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("BookKeeperClientScheduler"));
        client = new BookieClientImpl(new ClientConfiguration(), eventLoopGroup,
                UnpooledByteBufAllocator.DEFAULT, executor, scheduler, NullStatsLogger.INSTANCE,
                BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        
        addr = bs.getBookieId();
        wrongAddr =  BookieId.parse("127.0.0.2");
        result = new ResultStruct();

        passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        byteBuf = createByteBuffer(1, 1, 1);
        
        ledgerId = 1;
        entryId = 1;
        init_rc = -1000;			//some negative values are used for custom exceptions
    }
    
    //server configuration
    private static String getLoopbackInterfaceName() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nif : Collections.list(nifs)) {
                if (nif.isLoopback()) {
                    return nif.getName();
                }
            }
        } catch (SocketException se) {
        	System.out.println(se.getMessage());
            return null;
        }
        System.out.println("Unable to deduce loopback interface.");
        return null;
    }

    @After
    public void tearDown() throws Exception {
        scheduler.shutdown();
        bs.shutdown();
        recursiveDelete(tmpDir);
        eventLoopGroup.shutdownGracefully();
        executor.shutdown();
        client.close();
    }

    private static void recursiveDelete(File dir) {
        File[] children = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                recursiveDelete(child);
            }
        }
        dir.delete();
    }

    static class ResultStruct {
        int rc = init_rc;
        ByteBuffer entry;
    }

    ReadEntryCallback recb = new ReadEntryCallback() {

        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
            ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (BKException.Code.OK == rc && buffer != null) {
                	buffer.readerIndex(24);
                    rs.entry = buffer.nioBuffer();
                }
                rs.notifyAll();
            }
        }
    };

    WriteCallback wrcb = new WriteCallback() {
    	
        public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
            if (ctx != null) {
                synchronized (ctx) {
                    if (ctx instanceof ResultStruct) {
                        ResultStruct rs = (ResultStruct) ctx;
                        rs.rc = rc;
                    }
                    ctx.notifyAll();
                }
            }
        }
    };
    

    private ByteBufList createByteBuffer(int i, long lid, long eid) {
        ByteBuf byteBuf = Unpooled.buffer(4 + 24);
        byteBuf.writeLong(lid);				//ledger id
        byteBuf.writeLong(eid);				//entry id
        byteBuf.writeLong(eid - 1);			//entry id-1
        byteBuf.writeInt(i);				//
        return ByteBufList.get(byteBuf);
    }
    
    @Test
    public void testCorrectReadAndWrite() {
    
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(entryId, result.entry.getInt());
        }
    }
    
    @Test
    public void testNegativeEntryReadAndWrite() {
        
    	entryId = -3;
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        
        result.rc = init_rc;
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(init_rc, result.rc);			//result not written
        }
    }
    
    @Test
    public void testZeroEntryReadAndWrite() {
        
    	entryId = 0;
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        
        result.rc = init_rc;
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchEntryException, result.rc);		//result written but entryId not valid
        }
    }
    
    @Test
    public void testZeroLedgerReadAndWrite() {
        
    	ledgerId = 0;
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }
    
    @Test
    public void testNegativeLedgerReadAndWrite() {
        
    	ledgerId = -3;
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }
    
    @Test
    public void testTrueFastFail() {
        
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, true, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(entryId, result.entry.getInt());
        }
    }
    
    @Test
    public void testWrongEntry() {
    	
    	synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	entryId = 2;
    	synchronized (result) {
    		client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(BKException.Code.NoSuchEntryException, result.rc);
    	}
    }
    
    @Test
    public void testnullPassword() {
    	
    	synchronized (result) {
        	client.addEntry(addr, ledgerId, null, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(init_rc, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
    	}
    }
    
    @Test
    public void testnullWriteCallback() {
    	
    	synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, null, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(init_rc, result.rc);				// no one writes on result
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(entryId, result.entry.getInt());
    	}
    }
    
    @Test 
    public void testnullReadCallback() {
    	
    	synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	result.rc = init_rc;
    	synchronized (result) {
    		client.readEntry(addr, ledgerId, entryId, null, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(init_rc, result.rc);				//ledger written but no recb = no write on result
    	}
    }
    
    @Test
    public void testnullResult() {
    	
    	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(init_rc, result.rc);			//result as initial
    	
    	client.readEntry(addr, ledgerId, entryId, recb, null, BookieProtocol.FLAG_NONE);
    	assertEquals(init_rc, result.rc);
    }
    
    @Test
    public void testDeferred_Sync() {
    	
    	synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, EnumSet.of(WriteFlag.DEFERRED_SYNC));
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	
    	synchronized (result) {
    		client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(entryId, result.entry.getInt());
    	}
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullBuffer() {
    	
    	client.addEntry(addr, ledgerId, passwd, entryId, null, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    }
    
    @Test
    public void testEmptyBuffer() {
    	
    	byteBuf = ByteBufList.get(Unpooled.wrappedBuffer(new byte[0]));
    	synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.WriteException, result.rc);
    	}
    }
    
    @Test
    public void testNotValidBufferValue1() {
    	
    	byte[] hello = "hello".getBytes(UTF_8);
        byteBuf = ByteBufList.get(Unpooled.wrappedBuffer(hello));
        
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, 1, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.WriteException, result.rc);
        }
    }
    
    @Test
    public void testMultipleWrite() {
    	
    	entryId = 2;
    	byteBuf = createByteBuffer(entryId, ledgerId, entryId);
    	synchronized (result) {
    		client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    		 try {
 				result.wait(1000);
 			} catch (InterruptedException e) {
 				e.printStackTrace();
 			}
    	}
    	entryId = 3;
    	byteBuf = createByteBuffer(entryId, ledgerId, entryId);
    	synchronized (result) {
    		client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE); 
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	entryId = 5;
    	byteBuf = createByteBuffer(entryId, ledgerId, entryId);
    	synchronized (result) {
    		client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	entryId = 7;
    	byteBuf = createByteBuffer(entryId, ledgerId, entryId);
    	synchronized (result) {
    		client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, null, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
    		 try {
  				result.wait(1000);
  			} catch (InterruptedException e) {
  				e.printStackTrace();
  			}
    	}
    		 
    	synchronized (result) {
    		client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
    		try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		assertEquals(0, result.rc);
    		assertEquals(entryId, result.entry.getInt());
    	}
    }
    
    @Test
    public void testNoLedger() {
        
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.NoSuchLedgerExistsException, result.rc);
        }
    }
    
    
    @Test
    public void testClientWrong() {
    	
        synchronized (result) {
        	client.addEntry(wrongAddr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
        synchronized (result) {
            client.readEntry(wrongAddr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
    }
    
    @Test
    public void testClientClose() {		//to reach Branch Coverage
    	
    	client.close();					//closed = true
    	assertTrue(client.isClosed());
    	
        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
    }
    
    WriteLacCallback wrlcb = new WriteLacCallback() {
    	
		@Override
		public void writeLacComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
			if (ctx != null) {
                synchronized (ctx) {
                    if (ctx instanceof ResultStruct) {
                        ResultStruct rs = (ResultStruct) ctx;
                        rs.rc = rc;
                    }
                    ctx.notifyAll();
                }
            }
			
		}
    };
    
    ReadLacCallback rlcb = new ReadLacCallback() {

		@Override
		public void readLacComplete(int rc, long ledgerId, ByteBuf lac, ByteBuf buffer, Object ctx) {
			ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (BKException.Code.OK == rc && buffer != null) {
                	buffer.readerIndex(24);
                    rs.entry = buffer.nioBuffer();
                }
                rs.notifyAll();
            }
        }
    };
    
    ForceLedgerCallback flcb = new ForceLedgerCallback() {

		@Override
		public void forceLedgerComplete(int rc, long ledgerId, BookieId addr, Object ctx) {
			if (ctx != null) {
				synchronized (ctx) {
					if (ctx instanceof ResultStruct) {
						ResultStruct rs = (ResultStruct) ctx;
						rs.rc = rc;
					}
					ctx.notifyAll();
				}
			}
		}
    	
    };
    
    @Test
    public void testFaultyBookies() {
    	
    	client.lookupClient(addr);
    	
    	List<BookieId> faultyBookies = client.getFaultyBookies();
    	assertEquals(0, faultyBookies.size());
    }
    
    @Test
    public void testForceLedger() {

    	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        assertEquals(-1000, result.rc);
    	
        synchronized(result) {
        	client.forceLedger(addr, ledgerId, flcb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	assertEquals(0, result.rc);
        }
    }
    
    @Test
    public void testGetListOfEntriesOfLedger() {

        synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, EnumSet.of(WriteFlag.DEFERRED_SYNC));
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	assertNotNull(client.getListOfEntriesOfLedger(addr, ledgerId));
    	
    	client.close();
    	assertNotNull(client.getListOfEntriesOfLedger(addr, ledgerId));
    }
    
    @Test
    public void testPendingRequest() {

    	assertEquals(0, client.getNumPendingRequests(addr, ledgerId));

    	synchronized (result) {
        	client.addEntry(addr, ledgerId, passwd, entryId, byteBuf, wrcb, result, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        	assertEquals(0, client.getNumPendingRequests(addr, ledgerId), 1);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }
    	synchronized (result) {
            client.readEntry(addr, ledgerId, entryId, recb, result, BookieProtocol.FLAG_NONE);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
            assertEquals(entryId, result.entry.getInt());
        }
    	
    	client.close();
    	assertEquals(0, client.getNumPendingRequests(addr, 1));
    } 
    
    @Test
    public void ReadAndWriteLAC() {
    	
    	byteBuf = createByteBuffer(entryId, ledgerId, entryId);
          
    	synchronized (result) {
        	client.writeLac(addr, ledgerId, passwd, entryId, byteBuf, wrlcb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(0, result.rc);
        }   

        synchronized (result) {
        	client.readEntryWaitForLACUpdate(addr, ledgerId, entryId, 0, 1000, true, recb, result);
        	assertEquals(0, result.rc);
        }
        synchronized (result) {
            client.readLac(addr, ledgerId, rlcb, result);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ReadException, result.rc);
        }
    }
    
    @Test
    public void TestClientClose2() {
    	
    	client.close();					//closed = true
    	assertTrue(client.isClosed());
    	
    	synchronized(result) {
        	client.forceLedger(addr, ledgerId, flcb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        	assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
    	entryId = 5;
        synchronized (result) {
            client.writeLac(addr, ledgerId, passwd, entryId, byteBuf, wrlcb, result);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
            client.readLac(addr, ledgerId, rlcb, result);
            try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.ClientClosedException, result.rc);
        }
        
        synchronized (result) {
        	client.readEntryWaitForLACUpdate(addr, ledgerId, entryId, 0, 0, false, recb, result);
        	try {
				result.wait(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            assertEquals(BKException.Code.BookieHandleNotAvailableException, result.rc);
        }
    }
}
