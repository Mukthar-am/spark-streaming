package org.muks.insider.utils;

import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    public DateTimeItems currentTimestampItems() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        return new DateTimeItems()
                .setYear(cal.get(Calendar.YEAR))
                .setMonth(cal.get(Calendar.MONTH))
                .setDay(cal.get(Calendar.DAY_OF_MONTH))
                .setHour(cal.get(Calendar.HOUR_OF_DAY))
                .setMinutes(cal.get(Calendar.MINUTE))
                .setSeconds(cal.get(Calendar.SECOND))
                .setMilliseconds(cal.get(Calendar.MILLISECOND));
    }

    public static void main(String[] args) {
        System.out.println(new DateUtils().currentTimestampItems().toString());
    }

    public static class DateTimeItems {
        public int YEAR, MONTH, DAY, HOURS, MINUTES, SEONCDS, MILLSECONDS;

        public DateTimeItems setYear(int year) {
            this.YEAR = year;
            return this;
        }

        public DateTimeItems setMonth(int month) {
            this.MONTH = month;
            return this;
        }

        public DateTimeItems setDay(int day) {
            this.DAY = day;
            return this;
        }

        public DateTimeItems setHour(int hour) {
            this.HOURS = hour;
            return this;
        }

        public DateTimeItems setMinutes(int min) {
            this.MINUTES = min;
            return this;
        }

        public DateTimeItems setSeconds(int secs) {
            this.SEONCDS = secs;
            return this;
        }

        public DateTimeItems setMilliseconds(int milliseconds) {
            this.MILLSECONDS = milliseconds;
            return this;
        }

        public String toString() {
            return this.YEAR + "-" + this.MONTH + "-" + this.DAY + " " + this.HOURS + ":" + this.MINUTES + ":" + this.SEONCDS + "::" + this.MILLSECONDS;
        }
    }
}
