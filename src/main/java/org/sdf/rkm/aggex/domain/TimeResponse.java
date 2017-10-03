package org.sdf.rkm.aggex.domain;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TimeResponse implements Serializable {
    private static final long serialVersionUID = -7236011838695272562L;
    private Map<String, Object> now;
    private List<String> urls;

    public Map<String, Object> getNow() {
        return now;
    }

    public TimeResponse() {
    }

    public TimeResponse(Map<String, Object> now, List<String> urls) {
        this.now = now;
        this.urls = urls;
    }

    public List<String> getUrls() {
        return urls;
    }
}
