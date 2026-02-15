package pdc;

import java.io.*;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    
    public String magic = "CSM218"; 
    public int version = 1;
    public String messageType;
    public String studentId;
    public long timestamp = System.currentTimeMillis();
    public String payload = "";

    public Message(String type, String student, String data) {
        messageType = type;
        studentId = student;
        payload = data;
    }

    public byte[] pack() {
        try {
          
            ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
            DataOutputStream dataWriter = new DataOutputStream(messageStream);      
            dataWriter.writeUTF(magic);
            dataWriter.writeInt(version);
            dataWriter.writeUTF(messageType);
            dataWriter.writeUTF(studentId);
            dataWriter.writeLong(timestamp);
            dataWriter.writeUTF(payload);  
            byte[] messageBytes = messageStream.toByteArray();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataOutputStream outputWriter = new DataOutputStream(outputStream);
            outputWriter.writeInt(messageBytes.length);
            outputWriter.write(messageBytes);          
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

  
    public static Message unpack(byte[] data) throws Exception {

        DataInputStream dataReader = new DataInputStream(new ByteArrayInputStream(data));
        int messageLength = dataReader.readInt();
        byte[] messageBytes = new byte[messageLength];
        dataReader.readFully(messageBytes);
        DataInputStream messageReader = new DataInputStream(new ByteArrayInputStream(messageBytes));
        Message message = new Message("", "", "");
        message.magic = messageReader.readUTF();
        message.version = messageReader.readInt();
        message.messageType = messageReader.readUTF();
        message.studentId = messageReader.readUTF();
        message.timestamp = messageReader.readLong();
        message.payload = messageReader.readUTF();
        
        return message;
    }

  
    public void writeTo(OutputStream outputStream) throws Exception {
        outputStream.write(pack());
        outputStream.flush();
    }

 
    public static Message readFrom(InputStream inputStream) throws Exception {
        DataInputStream dataReader = new DataInputStream(inputStream);
        
    
        int messageLength = dataReader.readInt();
        byte[] messageBytes = new byte[messageLength];
        dataReader.readFully(messageBytes);
        
   
        DataInputStream messageReader = new DataInputStream(new ByteArrayInputStream(messageBytes));
        Message message = new Message("", "", "");
        message.magic = messageReader.readUTF();
        message.version = messageReader.readInt();
        message.messageType = messageReader.readUTF();
        message.studentId = messageReader.readUTF();
        message.timestamp = messageReader.readLong();
        message.payload = messageReader.readUTF();
        
        return message;
    }
}
