package com.cobub.es.client;

import java.util.List;

/**
 * Created by feng.wei on 2015/12/17.
 */
public class PutListBean {

    private String index;
    private String type;
    private String id;
    private List<String> sources;

    public PutListBean() {
    }

    public PutListBean(String index, String type, String id, List<String> sources) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.sources = sources;
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

    public List<String> getSources() {
        return sources;
    }

    public void setSources(List<String> sources) {
        this.sources = sources;
    }

    @Override
    public String toString() {
        return "PutListBean{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", sources=" + sources +
                '}';
    }
}
