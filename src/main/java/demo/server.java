package demo;

import org.zeromq.ZMQ;

import java.io.*;

public class server {
    ZMQ.Context context;
    ZMQ.Socket responder;
    String sendData;
    private static String ENCODING = "utf-8";
    private static String IP ="tcp://172.16.10.58:5555";

    private server() {
        context = ZMQ.context(1);
        responder = context.socket(ZMQ.REP);
        responder.bind(IP);
        sendData = "hello I`m server!";
    }

    private server(String data) {
        context = ZMQ.context(1);
        responder = context.socket(ZMQ.REP);

        this.sendData = data;
    }

    public void setSendData(String data) {
        sendData = data;
    }

    public void start() throws InterruptedException, UnsupportedEncodingException {

        while (!Thread.currentThread().isInterrupted()) {
            byte[] request = responder.recv(0);
            System.out.println("Received===>" + new String(request));
            Thread.sleep(1);
            responder.send(sendData.getBytes(ENCODING), 0);
        }
        responder.close();
        context.term();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        server server1 = new server();

        SerializedObject object = new SerializedObject();
        String message = serialize(object);
        //server1.setSendData(message);
        server1.start();

    }

    public static String serialize(Object obj) throws IOException, IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream;
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        String string = byteArrayOutputStream.toString(ENCODING);
        objectOutputStream.close();
        byteArrayOutputStream.close();
        return string;
    }

    public static Object serializeToObject(String str) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(str.getBytes(ENCODING));
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Object object = objectInputStream.readObject();
        objectInputStream.close();
        byteArrayInputStream.close();
        return object;
    }

}
