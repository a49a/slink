package hua.mulan.slink;

import org.apache.flink.table.annotation.DataTypeHint;
//import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/08/11
 **/
//@FunctionHint(output = @DataTypeHint("ROW<ct STRING>"))
public class Tf extends TableFunction<Row> {
    public void eval(String str) {
            collect(Row.of(str + "AAA"));
    }
}
