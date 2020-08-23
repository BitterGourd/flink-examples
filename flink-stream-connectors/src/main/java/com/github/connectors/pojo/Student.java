package com.github.connectors.pojo;

public class Student {

    private int userId;
    private String name;
    private double score;

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public static Student of(int userId, String name, double score) {
        Student student = new Student();
        student.setUserId(userId);
        student.setName(name);
        student.setScore(score);

        return student;
    }

    @Override
    public String toString() {
        return "Student{" +
                "userId=" + userId +
                ", name='" + name + '\'' +
                ", score=" + score +
                '}';
    }
}
