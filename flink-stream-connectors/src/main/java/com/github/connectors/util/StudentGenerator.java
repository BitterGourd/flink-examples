package com.github.connectors.util;

import com.github.connectors.pojo.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class StudentGenerator implements SourceFunction<Student> {

    private static final long serialVersionUID = -3545514884588221448L;

    private volatile boolean isRunning = true;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final long pause;
    private final int userIdBound;
    private final double scoreBound;

    public StudentGenerator(long pause, int userIdBound, double scoreBound) {
        this.pause = pause;
        this.userIdBound = userIdBound;
        this.scoreBound = scoreBound;
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        while (isRunning) {
            int userId = random.nextInt(userIdBound);
            ctx.collect(Student.of(userId, String.valueOf(userId), random.nextDouble(scoreBound)));

            Thread.sleep(pause);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
