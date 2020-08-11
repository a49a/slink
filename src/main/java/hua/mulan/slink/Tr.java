package hua.mulan.slink;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/11
 **/
public class Tr extends ScalarFunction {
    public String eval(String s) {
        return s.toUpperCase() + "GGG";
    }
}
