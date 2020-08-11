package com.github.streaming.join.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class WindowJoinSampleData {

    private static final String[] NAMES = {"tom", "jerry", "alice", "bob", "john", "grace"};
    private static final int GRADE_COUNT = 5;
    private static final int SALARY_MAX = 10000;

    /** 生成 (name, grade) */
    public static class GradeSource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private static final long serialVersionUID = -8363032590621252556L;
        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(GRADE_COUNT) + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(new ThrottledIterator<>(new GradeSource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}));
        }
    }

    /** 生成 (name, salary) */
    public static class SalarySource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private static final long serialVersionUID = -5011407003303641008L;
        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(SALARY_MAX) + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(new ThrottledIterator<>(new SalarySource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}));
        }
    }
}
