package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.InputStream;


public class Message {

  
    public static final byte TYPE_REGISTER  = 0;
    public static final byte TYPE_TASK      = 1;
    public static final byte TYPE_RESULT    = 2;
    public static final byte TYPE_HEARTBEAT = 3;
    public static final byte TYPE_ACK       = 4;


    private static final String MAGIC = "CSM218";  
    private static final int  MAGIC_NUMBER = 0xDEADBEEF; 
    private static final byte VERSION      = 1;  

    
    private final byte   type;      
    private final String sender;      
    private final int    taskId;
    private final byte[] payload;
    private       long   timestamp;

    public Message(byte type, String sender, int taskId, byte[] payload) {
        this.type      = type;
        this.sender    = sender  != null ? sender  : "";
        this.taskId    = taskId;
        this.payload   = payload != null ? payload : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    
    public byte[] serialize() {
        return pack();
    }

    
    public static Message deserialize(byte[] data) {
        return unpack(data);
    }

    
    public byte[] pack() {
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);

        int totalSize =
                4 +                   
                1 +                  
                1 +      
                2 +                   
                senderBytes.length + 
                4 +                
                4 +                   
                payload.length +      
                8;                   

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        buffer.putInt(MAGIC_NUMBER);
        buffer.put(VERSION);
        buffer.put(type);
        buffer.putShort((short) senderBytes.length);
        buffer.put(senderBytes);
        buffer.putInt(taskId);
        buffer.putInt(payload.length);
        buffer.put(payload);
        buffer.putLong(timestamp);

        return buffer.array();
    }

 
    public static Message unpack(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);

        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException("Invalid magic number — not a valid protocol message");
        }

        buffer.get();                      
        byte type = buffer.get();

        short senderLen  = buffer.getShort();
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        String sender = new String(senderBytes, StandardCharsets.UTF_8);

        int taskId = buffer.getInt();

        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);

        long timestamp = buffer.getLong();

        Message msg = new Message(type, sender, taskId, payload);
        msg.timestamp = timestamp; 
        return msg;
    }


    public static Message readFromStream(InputStream in) throws IOException {


        byte[] prefix = new byte[8];
        int bytesRead = 0;
        while (bytesRead < 8) {
            int n = in.read(prefix, bytesRead, 8 - bytesRead);
            if (n == -1) {
                if (bytesRead == 0) return null; // clean EOF between messages
                throw new IOException("Stream ended mid-header (read " + bytesRead + "/8 bytes)");
            }
            bytesRead += n;
        }

        ByteBuffer prefixBuf = ByteBuffer.wrap(prefix);
        prefixBuf.order(ByteOrder.BIG_ENDIAN);
        prefixBuf.getInt();  
        prefixBuf.get();     
        prefixBuf.get();  
        short senderLen = prefixBuf.getShort();

        int restHeaderLen = senderLen + 4 + 4;
        byte[] restHeader = new byte[restHeaderLen];
        bytesRead = 0;
        while (bytesRead < restHeaderLen) {
            int n = in.read(restHeader, bytesRead, restHeaderLen - bytesRead);
            if (n == -1) throw new IOException("Stream ended mid-header");
            bytesRead += n;
        }

  
        ByteBuffer restBuf = ByteBuffer.wrap(restHeader);
        restBuf.order(ByteOrder.BIG_ENDIAN);
        restBuf.position(senderLen + 4); 
        int payloadLen = restBuf.getInt();


        byte[] payload = new byte[payloadLen];
        bytesRead = 0;
        while (bytesRead < payloadLen) {
            int n = in.read(payload, bytesRead, payloadLen - bytesRead);
            if (n == -1) throw new IOException("Stream ended mid-payload");
            bytesRead += n;
        }


        byte[] timestampBytes = new byte[8];
        bytesRead = 0;
        while (bytesRead < 8) {
            int n = in.read(timestampBytes, bytesRead, 8 - bytesRead);
            if (n == -1) throw new IOException("Stream ended mid-timestamp");
            bytesRead += n;
        }

   
        int total = 8 + restHeaderLen + payloadLen + 8;
        ByteBuffer full = ByteBuffer.allocate(total);
        full.order(ByteOrder.BIG_ENDIAN);
        full.put(prefix);
        full.put(restHeader);
        full.put(payload);
        full.put(timestampBytes);

        return unpack(full.array());
    }

  
    public byte   getType()      { return type;      }
    public String getSender()    { return sender;     }
    public int    getTaskId()    { return taskId;     }
    public byte[] getPayload()   { return payload;    }
    public long   getTimestamp() { return timestamp;  }
}
