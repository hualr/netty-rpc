package com.netty.rpc.protocol;

import com.netty.rpc.util.JsonUtil;
import java.io.Serializable;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class RpcServiceInfo implements Serializable {
    // interface name
    private String serviceName;
    // service version
    private String version;

    //TODO DELETE
    public String toJson() {
        return JsonUtil.objectToJson(this);
    }

    @Override
    public String toString() {
        return  JsonUtil.objectToJson(this);
    }
}
