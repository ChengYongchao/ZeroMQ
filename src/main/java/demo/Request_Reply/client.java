package demo.Request_Reply;

import demo.SerializedObject;
import org.zeromq.ZMQ;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;

public class client {
    ZMQ.Context context;
    ZMQ.Socket requester;
    private static String IP ="tcp://*:5555";
    private static String ENCODING = "GBK";

    private client() {
        context = ZMQ.context(1);
        requester = context.socket(ZMQ.REQ);
        requester.connect(IP);
    }

    public void start() throws IOException, ClassNotFoundException {
        for (int requestNbr = 0; requestNbr != 100; requestNbr++) {
            String request = "Hello I`m client";
            System.out.println("Sending Hello: " + requestNbr);
            requester.send(request.getBytes(), 0);
            byte[] reply = requester.recv(0);
            System.out.println("Reveived: " + new String(reply) + " " + requestNbr);

            //序列化测试
//            String data = new String(reply,ENCODING);
//            SerializedObject res = (SerializedObject)serializeToObject(data);
//            System.out.println("Reveived: " + res + " " + requestNbr);
        }
        requester.close();
        context.term();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        client client1 = new client();
        client1.start();

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
