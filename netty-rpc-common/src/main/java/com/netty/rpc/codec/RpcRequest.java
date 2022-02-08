package com.netty.rpc.codec;

import java.io.Serializable;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * RPC Request
 *
 * @author luxiaoxun
 */
@Data
@Accessors(chain = true)
public class RpcRequest implements Serializable {
    private static final long serialVersionUID = -2524587347775862771L;
    private String requestId;
    private String className;
    private String methodName;
    private Class<?>[] parameterTypes;
    private Object[] parameters;
    private String version;

}