package demo;

import org.zeromq.ZStar;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SerializedObject implements Serializable {
    private static final long serialVersionUID = 362498818548588607L;
    public String message;

    public SerializedObject() {
        message = "hello word!";

    }

    public SerializedObject(Boolean bool) {
         //message = readToString("C:\\Users\\Administrator\\Desktop\\bigdata.txt");
    }

    @Override
    public String toString() {
        return "序列化消息===>" + message;
    }

    public static String readToString(String fileName) {
        String encoding = "UTF-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        System.out.println(readToString("C:\\Users\\Administrator\\Desktop\\bigdata.txt"));
    }
}
