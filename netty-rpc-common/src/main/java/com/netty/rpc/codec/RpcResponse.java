package com.netty.rpc.codec;

import java.io.Serializable;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * RPC Response
 *
 * @author luxiaoxun
 */
@Data
@Accessors(chain = true)
public class RpcResponse implements Serializable {
    private static final long serialVersionUID = 8215493329459772524L;

    private String requestId;
    private String error;
    private Object result;

    public boolean isError() {
        return error != null;
    }

}
