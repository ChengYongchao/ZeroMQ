package demo;

import java.io.Serializable;

public class SerializedObject implements Serializable {
    public String message;

    public SerializedObject() {
        message = "hello word!";
    }

    @Override
    public String toString() {
        return "序列化消息===>" + message;
    }
}
