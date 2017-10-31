package com.cobub.es.client;

import java.util.List;

/**
 * Created by feng.wei on 2015/12/17.
 */
public class PutBean {

    private String index;
    private String type;
    private String id;
    private String source;

    public PutBean() {
    }

    public PutBean(String index, String type, String source) {
        this.index = index;
        this.type = type;
        this.source = source;
    }

    public PutBean(String index, String type, String id, String source) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return "PutBean{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", source='" + source + '\'' +
                '}';
    }
}
