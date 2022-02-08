package com.netty.rpc.protocol;

import com.netty.rpc.util.JsonUtil;
import java.io.Serializable;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 一个服务器下的服务信息
 */
@Data
@Accessors(chain = true)
public class ServerConfigInfo implements Serializable {
    private static final long serialVersionUID = -1102180003395190700L;
    // service host
    private String host;
    // service port
    private int port;
    // service info list
    private List<RpcServiceInfo> serviceInfoList;

    public String toJson() {
        return JsonUtil.objectToJson(this);
    }

    public static ServerConfigInfo fromJson(String json) {
        return JsonUtil.jsonToObject(json, ServerConfigInfo.class);
    }


    @Override
    public String toString() {
        return toJson();
    }

}
