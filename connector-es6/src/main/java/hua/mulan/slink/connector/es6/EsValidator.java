package hua.mulan.slink.connector.es6;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/11
 **/
public class EsValidator extends ConnectorDescriptorValidator {
    
    public static final String CONNECTOR_ADDRESS = "address";
    public static final String CONNECTOR_CLUSTER = "cluster";
    public static final String CONNECTOR_INDEX = "index";
    public static final String CONNECTOR_DOCUMENT_TYPE = "estype";
    public static final String CONNECTOR_ID = "id";
    public static final String CONNECTOR_AUTHMESH = "authMesh";
    public static final String CONNECTOR_USERNAME = "userName";
    public static final String CONNECTOR_PASSWORD = "password";
    public static final String KEY_TRUE = "true";
    public static final String CONNECTOR_PARALLELISM = "parallelism";

}
