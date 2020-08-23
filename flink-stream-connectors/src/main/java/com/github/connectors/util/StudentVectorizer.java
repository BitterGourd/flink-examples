package com.github.connectors.util;

import com.github.connectors.pojo.Student;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * 将 Student 类转换为 {@link VectorizedRowBatch}.
 */
public class StudentVectorizer extends Vectorizer<Student> implements Serializable {

    private static final long serialVersionUID = -1181847572636391258L;

    public StudentVectorizer(String schema) {
        super(schema);
    }

    @Override
    public void vectorize(Student element, VectorizedRowBatch batch) throws IOException {
        LongColumnVector userIdColVector = (LongColumnVector) batch.cols[0];
        BytesColumnVector nameColVector = (BytesColumnVector) batch.cols[1];
        DoubleColumnVector scoreColVector = (DoubleColumnVector) batch.cols[2];

        int row = batch.size++;
        userIdColVector.vector[row] = element.getUserId();
        nameColVector.setVal(row, element.getName().getBytes(StandardCharsets.UTF_8));
        scoreColVector.vector[row] = element.getScore();

        // 想要基于输入或外部系统动态添加新元数据可以在这里调用 addUserMetadata 方法
        // this.addUserMetadata(key, value);
    }
}
