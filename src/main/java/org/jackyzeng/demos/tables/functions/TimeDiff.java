package org.jackyzeng.demos.tables.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

// 标量函数（Scalar Function）接收零个、一个或者多个输入，输出一个单值标量。
public class TimeDiff extends ScalarFunction {

    public @DataTypeHint("BIGINT") long eval(Timestamp first, Timestamp second) {
        return java.time.Duration.between(first.toInstant(), second.toInstant()).toMillis();
    }
}
