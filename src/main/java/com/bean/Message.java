package com.bean;


/**
 * record a blog
 */
public class Message {

    private String uid;
    private String timestamp;
    private String content;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Message [作者:" + uid + ", 发布时间:" + timestamp + ", 内容:" + content + "]";
    }

}
