package ch.ethz.tell;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.*;

public class TellStore extends DB {

  static byte[] READ;
  static byte[] SCAN;
  static byte[] UPDATE;
  static byte[] INSERT;
  static byte[] DELETE;
  static Charset CHARSET = Charset.forName("UTF-8");

  static byte[] toLittleEndian(int value) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(value);
    return bb.array();
  }

  static {
    READ = toLittleEndian(1);
    SCAN = toLittleEndian(2);
    UPDATE = toLittleEndian(3);
    INSERT = toLittleEndian(4);
    DELETE = toLittleEndian(5);
  }

  Socket clientSocket;
  BufferedOutputStream out;
  BufferedInputStream in;

  @Override
  public final void init() {
    Properties props = getProperties();
    String host = props.getProperty("ycsb-tell.server", "localhost");
    String port = props.getProperty("ycsb-tell.server-port", "8713");
    try {
      clientSocket = new Socket(host, Integer.parseInt(port));
      out = new BufferedOutputStream(clientSocket.getOutputStream());
      in = new BufferedInputStream(clientSocket.getInputStream());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public final void cleanup() {
    try {
      clientSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeString(OutputStream out, String str) throws IOException {
    out.write(toLittleEndian(str.length()));
    out.write(str.getBytes(CHARSET));
  }

  public static int byteArrayToInt(byte[] b)
  {
    return   b[3] & 0xFF |
      (b[2] & 0xFF) << 8 |
      (b[1] & 0xFF) << 16 |
      (b[0] & 0xFF) << 24;
  }

  private int readInt() throws IOException {
    byte[] result = new byte[4];
    if (in.read(result) != 4) {
      throw new RuntimeException();
    }
    return byteArrayToInt(result);
  }

  private String readString() throws IOException {
    byte[] str = readByteArray();
    return new String(str, CHARSET);
  }

  private void writeMap(OutputStream out, Map<String, ByteIterator> map) throws IOException {
    out.write(toLittleEndian(map.size()));
    for (Map.Entry<String, ByteIterator> entry : map.entrySet()) {
      writeString(out, entry.getKey());
      byte[] bytes = entry.getKey().getBytes();
      out.write(toLittleEndian(bytes.length));
      out.write(bytes);
    }
  }

  private void writeSet(OutputStream out, Set<String> set) throws IOException {
    out.write(toLittleEndian(set.size()));
    for (String str : set) {
      writeString(out, str);
    }
  }

  private Status readStatus() throws IOException {
    byte res[] = new byte[1];
    int read = in.read(res);
    if (read != 1) {
      throw new RuntimeException();
    }
    if (res[0] == 0) {
      return Status.OK;
    } else if (res[0] == 2) {
      return Status.NOT_FOUND;
    } else {
      return Status.ERROR;
    }
  }

  byte[] readByteArray() throws IOException {
    int length = readInt();
    byte[] result = new byte[length];
    int read = 0;
    while (read < length) {
      read += in.read(result, read, result.length - read);
    }
    return result;
  }

  ByteIterator readByteIterator() throws IOException {
    int lenghth = readInt();
    byte[] res = readByteArray();
    return new ByteArrayByteIterator(res);
  }

  private Status readMap(HashMap<String, ByteIterator> result) throws IOException {
    byte[] arr = new byte[1024];
    int length = readInt();
    for (int i = 0; i < length; ++i) {
      result.put(readString(), readByteIterator());
    }
    return Status.OK;
  }

  @Override
  public final Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      ByteArrayOutputStream resStream = new ByteArrayOutputStream();
      resStream.write(READ);
      writeString(resStream, table);
      writeString(resStream, key);
      writeSet(resStream, fields);
      byte[] req = resStream.toByteArray();
      out.write(toLittleEndian(req.length));
      out.write(req);
      out.flush();
      return readMap(result);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public final Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public final Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return generalUpdate(INSERT, table, key, values);
  }

  private Status generalUpdate(byte[] op, String table, String key, HashMap<String, ByteIterator> values) {
    try {
      ByteArrayOutputStream resStream = new ByteArrayOutputStream();
      resStream.write(op);
      writeString(resStream, table);
      writeString(resStream, key);
      writeMap(resStream, values);
      byte[] req = resStream.toByteArray();
      out.write(toLittleEndian(req.length));
      out.write(req);
      out.flush();
      return readStatus();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public final Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return generalUpdate(UPDATE, table, key, values);
  }

  @Override
  public final Status delete(String table, String key) {
    try {
      ByteArrayOutputStream resStream = new ByteArrayOutputStream();
      resStream.write(DELETE);
      writeString(resStream, table);
      writeString(resStream, key);
      byte[] req = resStream.toByteArray();
      out.write(toLittleEndian(req.length));
      out.write(req);
      out.flush();
      return readStatus();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
