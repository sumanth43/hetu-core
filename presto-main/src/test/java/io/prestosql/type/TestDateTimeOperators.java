/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.type;

import io.prestosql.Session;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.operator.scalar.FunctionAssertions;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.joda.time.DateTimeZone.UTC;

public class TestDateTimeOperators
        extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone WEIRD_TIME_ZONE = DateTimeZone.forOffsetHoursMinutes(5, 9);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(5 * 60 + 9);

    public TestDateTimeOperators()
    {
        super(testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testTimeZoneGap()
    {
        // No time zone gap should be applied

        assertFunction(
                "TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 31, 1, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 31, 2, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 31, 3, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 31, 1, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 31, 1, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 31, 0, 5, 0, 0));
    }

    @Test
    public void testDaylightTimeSaving()
    {
        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 1, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 2, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 3, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 4, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 26, 23, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 0, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 0, 5, 0, 0));

        assertFunction(
                "TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 2, 5, 0, 0));
        assertFunction(
                "TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour",
                TIMESTAMP,
                sqlTimestampOf(2013, 10, 27, 1, 5, 0, 0));
    }

    @Test
    public void testTimeWithTimeZoneRepresentation()
    {
        // PST -> PDT date
        testTimeRepresentationOnDate(
                new DateTime(2017, 3, 12, 10, 0, 0, 0, UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(
                new DateTime(2017, 3, 12, 10, 0, 0, 0, UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PDT -> PST date
        testTimeRepresentationOnDate(
                new DateTime(2017, 10, 4, 10, 0, 0, 0, UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(
                new DateTime(2017, 10, 4, 10, 0, 0, 0, UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PDT date
        testTimeRepresentationOnDate(
                new DateTime(2017, 6, 6, 10, 0, 0, 0, UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(
                new DateTime(2017, 6, 6, 10, 0, 0, 0, UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));

        // PST date
        testTimeRepresentationOnDate(
                new DateTime(2017, 11, 1, 10, 0, 0, 0, UTC),
                "TIME '02:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(37800000, getTimeZoneKey("America/Los_Angeles")));
        testTimeRepresentationOnDate(
                new DateTime(2017, 11, 1, 10, 0, 0, 0, UTC),
                "TIME '03:30:00.000 America/Los_Angeles'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(41400000, getTimeZoneKey("America/Los_Angeles")));
    }

    @Test
    public void testTimeRepresentation()
    {
        // PST -> PDT date
        testTimeRepresentationOnDate(new DateTime(2017, 3, 12, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(9000000));
        testTimeRepresentationOnDate(new DateTime(2017, 3, 12, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(12600000));

        // PDT -> PST date
        testTimeRepresentationOnDate(new DateTime(2017, 10, 4, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(9000000));
        testTimeRepresentationOnDate(new DateTime(2017, 10, 4, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(12600000));

        // PDT date
        testTimeRepresentationOnDate(new DateTime(2017, 6, 6, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(9000000));
        testTimeRepresentationOnDate(new DateTime(2017, 6, 6, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(12600000));

        // PST date
        testTimeRepresentationOnDate(new DateTime(2017, 11, 1, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '02:30:00.000'", TIME, new SqlTime(9000000));
        testTimeRepresentationOnDate(new DateTime(2017, 11, 1, 10, 0, 0, 0, DateTimeZone.UTC), "TIME '03:30:00.000'", TIME, new SqlTime(12600000));
    }

    private static void testTimeRepresentationOnDate(DateTime date, String timeLiteral, Type expectedType, Object expected)
    {
        Session localSession = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("America/Los_Angeles"))
                .setStartTime(date.getMillis())
                .build();

        try (FunctionAssertions localAssertions = new FunctionAssertions(localSession)) {
            localAssertions.assertFunction(timeLiteral, expectedType, expected);
            localAssertions.assertFunctionString(timeLiteral, expectedType, valueFromLiteral(timeLiteral));
        }
    }

    private static String valueFromLiteral(String literal)
    {
        Pattern p = Pattern.compile("'(.*)'");
        Matcher m = p.matcher(literal);
        verify(m.find());
        return m.group(1);
    }

    @Test
    public void testDatePlusInterval()
    {
        assertFunction("DATE '2001-1-22' + INTERVAL '3' day", DATE, toDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' day + DATE '2001-1-22'", DATE, toDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, UTC)));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' month", DATE, toDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' month + DATE '2001-1-22'", DATE, toDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, UTC)));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' year", DATE, toDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' year + DATE '2001-1-22'", DATE, toDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, UTC)));

        assertInvalidFunction("DATE '2001-1-22' + INTERVAL '3' hour", "Cannot add hour, minutes or seconds to a date");
        assertInvalidFunction("INTERVAL '3' hour + DATE '2001-1-22'", "Cannot add hour, minutes or seconds to a date");
    }

    @Test
    public void testTimePlusInterval()
    {
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' hour", TIME, sqlTimeOf(6, 4, 5, 321));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321'", TIME, sqlTimeOf(6, 4, 5, 321));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' day", TIME, sqlTimeOf(3, 4, 5, 321));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321'", TIME, sqlTimeOf(3, 4, 5, 321));

        assertFunction("TIME '03:04:05.321' + INTERVAL '27' hour", TIME, sqlTimeOf(6, 4, 5, 321));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321'", TIME, sqlTimeOf(6, 4, 5, 321));

        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' day",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '27' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimestampPlusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' hour",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 6, 4, 5, 321));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 6, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' day",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 25, 3, 4, 5, 321));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 25, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' month",
                TIMESTAMP,
                sqlTimestampOf(2001, 4, 22, 3, 4, 5, 321));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                sqlTimestampOf(2001, 4, 22, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' year",
                TIMESTAMP,
                sqlTimestampOf(2004, 1, 22, 3, 4, 5, 321));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                sqlTimestampOf(2004, 1, 22, 3, 4, 5, 321));

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' hour",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' day",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' month",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' year",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testDateMinusInterval()
    {
        assertFunction("DATE '2001-1-22' - INTERVAL '3' day", DATE, toDate(new DateTime(2001, 1, 19, 0, 0, 0, 0, UTC)));

        assertInvalidFunction("DATE '2001-1-22' - INTERVAL '3' hour", "Cannot subtract hour, minutes or seconds from a date");
    }

    @Test
    public void testTimeMinusInterval()
    {
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' hour", TIME, sqlTimeOf(0, 4, 5, 321));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' day", TIME, sqlTimeOf(3, 4, 5, 321));

        assertFunction("TIME '03:04:05.321' - INTERVAL '6' hour", TIME, sqlTimeOf(21, 4, 5, 321));

        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 0, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' day",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '6' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 21, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimestampMinusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' day",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 19, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' day",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 19, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' month",
                TIMESTAMP,
                sqlTimestampOf(2000, 10, 22, 3, 4, 5, 321));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' month",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2000, 10, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testDateToTimestampCoercing()
    {
        assertFunction("date_format(DATE '2013-10-27', '%Y-%m-%d %H:%i:%s')", VARCHAR, "2013-10-27 00:00:00");

        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59'", BOOLEAN, true);
    }

    @Test
    public void testDateToTimestampWithZoneCoercing()
    {
        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00 Europe/Berlin'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01 Europe/Berlin'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59 Europe/Berlin'", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS DATE) IS DISTINCT FROM CAST(NULL AS DATE)", BOOLEAN, false);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM TIMESTAMP '2013-10-27 00:00:00'", BOOLEAN, false);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM TIMESTAMP '2013-10-28 00:00:00'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM DATE '2013-10-27'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    @Test
    public void testDateCastFromVarchar()
    {
        assertFunction("DATE '2013-02-02'", DATE, toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));
        assertInvalidFunction("DATE '392251590-07-12'", INVALID_CAST_ARGUMENT, "Value cannot be cast to date: 392251590-07-12");
    }

    @Test
    public void testDateCastParse()
    {
        assertInvalidFunction("DATE '5881580-07-12'", INVALID_CAST_ARGUMENT, "Value cannot be cast to date: 5881580-07-12");
    }

    private static SqlDate toDate(DateTime dateTime)
    {
        return new SqlDate((int) TimeUnit.MILLISECONDS.toDays(dateTime.getMillis()));
    }
}
