// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.parser.trino;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TrinoFunctionTransformTest extends TrinoTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    @Test
    public void testAggFnTransform() throws Exception {
        String sql = "select approx_distinct(v1) from t0; ";
        assertPlanContains(sql, "output: approx_count_distinct(1: v1)");

        sql = "select arbitrary(v1) from t0; ";
        assertPlanContains(sql, "output: any_value(1: v1)");

        sql = "select approx_percentile(v1, 0.99) from t0;";
        assertPlanContains(sql, "output: percentile_approx(CAST(1: v1 AS DOUBLE), 0.99)");

        sql = "select stddev(v1) from t0;";
        assertPlanContains(sql, "output: stddev_samp(1: v1)");

        sql = "select stddev_pop(v1) from t0;";
        assertPlanContains(sql, "output: stddev(1: v1)");

        sql = "select variance(v1) from t0;";
        assertPlanContains(sql, "output: var_samp(1: v1)");

        sql = "select var_pop(v1) from t0;";
        assertPlanContains(sql, "output: variance(1: v1)");

        sql = "select count_if(v1) from t0;";
        assertPlanContains(sql, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(CAST(1: v1 AS BOOLEAN), 1, NULL))");
    }

    @Test
    public void testArrayFnTransform() throws Exception {
        String sql = "select array_union(c1, c2) from test_array";
        assertPlanContains(sql, "array_distinct(array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR");

        sql = "select concat(array[1,2,3], array[4,5,6]) from test_array";
        assertPlanContains(sql, "array_concat([1,2,3], [4,5,6])");

        sql = "select concat(c1, c2) from test_array";
        assertPlanContains(sql, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR>))");

        sql = "select concat(c1, c2, array[1,2], array[3,4]) from test_array";
        assertPlanContains(sql, "array_concat(2: c1, CAST(3: c2 AS ARRAY<VARCHAR>), " +
                "CAST([1,2] AS ARRAY<VARCHAR>), " +
                "CAST([3,4] AS ARRAY<VARCHAR>)");

        sql = "select concat(c2, 2) from test_array";
        assertPlanContains(sql, "array_concat(3: c2, CAST([2] AS ARRAY<INT>))");

        sql = "select contains(array[1,2,3], 1)";
        assertPlanContains(sql, "array_contains([1,2,3], 1)");

        sql = "select slice(array[1,2,3,4], 2, 2)";
        assertPlanContains(sql, "array_slice([1,2,3,4], 2, 2)");

        sql = "select contains_sequence(array[1,2,3], array[1,2])";
        assertPlanContains(sql, "array_contains_seq([1,2,3], [1,2])");

        sql = "select concat_ws('_', array['1','2','3'])";
        assertPlanContains(sql, "concat_ws('_', ['1','2','3'])");

        sql = "select concat_ws('|', array_agg(event_type)) from "
            + "(select 1 as event_type union all select 1 as event_type union all select 2 as event_type)";
        assertPlanContains(sql, "concat_ws('|', CAST(8: array_agg AS ARRAY<VARCHAR>))");

        sql = "select concat_ws('.', 5,6,'s',8,9,10)";
        assertPlanContains(sql, "'5.6.s.8.9.10'");

        sql = "select array_agg(v1) from t0";
        assertPlanContains(sql, "array_agg(1: v1)");

        sql = "select array_agg(distinct v1) from t0";
        assertPlanContains(sql, "array_agg_distinct(1: v1)");

        sql = "select array_agg(distinct v1 order by v2) from t0";
        assertPlanContains(sql, "array_agg(1: v1, 2: v2)");
    }

    @Test
    public void testArrayFnWithLambdaExpr() throws Exception {
        String sql = "select filter(array[], x -> true);";
        assertPlanContains(sql, "array_filter(CAST([] AS ARRAY<BOOLEAN>), array_map(<slot 2> -> TRUE, []))");

        sql = "select filter(array[5, -6, NULL, 7], x -> x > 0);";
        assertPlanContains(sql, " array_filter([5,-6,NULL,7], array_map(<slot 2> -> <slot 2> > 0, [5,-6,NULL,7]))");

        sql = "select filter(array[5, NULL, 7, NULL], x -> x IS NOT NULL);";
        assertPlanContains(sql, "array_filter([5,NULL,7,NULL], array_map(<slot 2> -> <slot 2> IS NOT NULL, [5,NULL,7,NULL]))");

        sql = "select array_sort(array[1, 2, 3])";
        assertPlanContains(sql, "array_sort([1,2,3])");
    }

    @Test
    public void testDateFnTransform() throws Exception {
        String sql = "select to_unixtime(TIMESTAMP '2023-04-22 00:00:00');";
        assertPlanContains(sql, "1682092800");

        sql = "select from_unixtime(1724049401);";
        assertPlanContains(sql, "2024-08-19 14:36:41");

        sql = "select from_unixtime(1724049401, 'America/Bogota');";
        assertPlanContains(sql, "2024-08-19 01:36:41");

        sql = "select from_unixtime(1724049401, 1, 1);";
        assertPlanContains(sql, "2024-08-19 15:37:41");

        sql = "select at_timezone(TIMESTAMP '2024-08-19 14:36:41', 'America/Bogota');";
        assertPlanContains(sql, "2024-08-19 01:36:41");

        sql = "select date_parse('2022/10/20/05', '%Y/%m/%d/%H');";
        assertPlanContains(sql, "2022-10-20 05:00:00");

        sql = "SELECT date_parse('20141221','%Y%m%d');";
        assertPlanContains(sql, "'2014-12-21'");

        sql = "select date_parse('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s');";
        assertPlanContains(sql, "2014-12-21 12:34:56");

        sql = "select day_of_week(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofweek_iso('2022-03-06 01:02:03')");

        sql = "select dow(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofweek_iso('2022-03-06 01:02:03')");

        sql = "select dow(date '2022-03-06');";
        assertPlanContains(sql, "dayofweek_iso('2022-03-06 00:00:00')");

        sql = "select day_of_month(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofmonth('2022-03-06 01:02:03')");

        sql = "select day_of_month(date '2022-03-06');";
        assertPlanContains(sql, "dayofmonth('2022-03-06 00:00:00')");

        sql = "select day_of_year(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofyear('2022-03-06 01:02:03')");

        sql = "select day_of_year(date '2022-03-06');";
        assertPlanContains(sql, "dayofyear('2022-03-06 00:00:00')");

        sql = "select doy(timestamp '2022-03-06 01:02:03');";
        assertPlanContains(sql, "dayofyear('2022-03-06 01:02:03')");

        sql = "select doy(date '2022-03-06');";
        assertPlanContains(sql, "dayofyear('2022-03-06 00:00:00')");

        sql = "select week_of_year(timestamp '2023-01-01 00:00:00');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select week(timestamp '2023-01-01');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select week_of_year(date '2023-01-01');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select week(date '2023-01-01');";
        assertPlanContains(sql, "week_iso('2023-01-01 00:00:00')");

        sql = "select format_datetime(TIMESTAMP '2023-06-25 11:10:20', 'yyyyMMdd HH:mm:ss')";
        assertPlanContains(sql, "20230625 11:10:20");

        sql = "select format_datetime(date '2023-06-25', 'yyyyMMdd HH:mm:ss');";
        assertPlanContains(sql, "20230625 00:00:00");

        sql = "select to_char(TIMESTAMP '2023-06-25 11:10:20', 'yyyyMMdd HH:mm:ss');";
        assertPlanContains(sql, "20230625 11:10:20");

        sql = "select parse_datetime('2023-08-02 14:37:02', 'yyyy-MM-dd HH:mm:ss')";
        assertPlanContains(sql, "str_to_jodatime('2023-08-02 14:37:02', 'yyyy-MM-dd HH:mm:ss')");

        sql = "select parse_datetime('2023-05','yyyy-MM')";
        assertPlanContains(sql, "str_to_jodatime('2023-05', 'yyyy-MM')");

        sql = "select parse_datetime('2023-08-02T14:37:02', 'yyyy-MM-dd''T''HH:mm:ss''Z''')";
        assertPlanContains(sql, "str_to_jodatime('2023-08-02T14:37:02', 'yyyy-MM-ddTHH:mm:ss')");

        connectContext.getSessionVariable().setDisableFunctionFoldConstants(true);
        sql = "select last_day_of_month(timestamp '2023-07-01 00:00:00');";
        assertPlanContains(sql, "last_day('2023-07-01 00:00:00', 'month')");

        sql = "select last_day_of_month(date '2023-07-01');";
        assertPlanContains(sql, "last_day('2023-07-01', 'month')");
        connectContext.getSessionVariable().setDisableFunctionFoldConstants(false);

        sql = "select date_diff('month', timestamp '2023-07-31', timestamp '2023-08-01');";
        assertPlanContains(sql, "date_diff('month', '2023-08-01 00:00:00', '2023-07-31 00:00:00')");

        sql = "select date_diff('month', timestamp '2023-07-31')";
        analyzeFail(sql, "date_diff function must have 3 arguments");

        sql = "select to_date('2022-02-02', 'yyyy-mm-dd')";
        assertPlanContains(sql, "to_tera_date('2022-02-02', 'yyyy-mm-dd')");

        sql = "select to_timestamp('2022-02-02', 'yyyy-mm-dd')";
        assertPlanContains(sql, " to_tera_timestamp('2022-02-02', 'yyyy-mm-dd')");

        sql = "select year_of_week('2022-02-02')";
        assertPlanContains(sql, "<slot 2> : 2022");

        sql = "select yow('2022-02-02')";
        assertPlanContains(sql, "<slot 2> : 2022");

        sql = "select from_iso8601_timestamp('2025-02-02 14:37:02')";
        assertPlanContains(sql, "'2025-02-02 14:37:02'");

        sql = "select from_iso8601_timestamp('2025-02-02')";
        assertPlanContains(sql, "'2025-02-02 00:00:00'");
    }

    @Test
    public void testDateAddFnTransform() throws Exception {
        String sql = "select date_add('day', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-09 09:00:00");

        sql = "select date_add('second', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 09:00:01");

        sql = "select date_add('minute', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 09:01:00");

        sql = "select date_add('hour', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 10:00:00");

        sql = "select date_add('week', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-15 09:00:00");

        sql = "select date_add('month', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "'2014-04-08 09:00:00'");

        sql = "select date_add('year', 1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "'2015-03-08 09:00:00'");

        sql = "select date_add('second', 2, th) from tall;";
        assertPlanContains(sql, "seconds_add(8: th, 2)");

        sql = "select date_add('minute', 2, th) from tall;";
        assertPlanContains(sql, "minutes_add(8: th, 2)");

        sql = "select date_add('hour', 2, th) from tall;";
        assertPlanContains(sql, "hours_add(8: th, 2)");

        sql = "select date_add('day', 2, th) from tall;";
        assertPlanContains(sql, "days_add(8: th, 2)");

        sql = "select date_add('week', 2, th) from tall;";
        assertPlanContains(sql, "weeks_add(8: th, 2)");

        sql = "select date_add('month', 2, th) from tall;";
        assertPlanContains(sql, "months_add(8: th, 2)");

        sql = "select date_add('year', 2, th) from tall;";
        assertPlanContains(sql, "years_add(8: th, 2)");

        sql = "select date_add('quarter', 2, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-09-08 09:00:00");

        sql = "select date_add('quarter', -1, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2013-12-08 09:00:00");

        sql = "select date_add('millisecond', 20, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 09:00:00.020000");

        sql = "select date_add('millisecond', -100, TIMESTAMP '2014-03-08 09:00:00');";
        assertPlanContains(sql, "2014-03-08 08:59:59.900000");
    }

    @Test
    public void testStringFnTransform() throws Exception {
        String sql = "select chr(56)";
        assertPlanContains(sql, "char(56)");

        sql = "select codepoint('a')";
        assertPlanContains(sql, "ascii('a')");

        sql = "select position('aa' in 'bccaab');";
        assertPlanContains(sql, "locate('aa', 'bccaab')");

        sql = "select strpos('bccaab', 'aa');";
        assertPlanContains(sql, "locate('aa', 'bccaab')");

        sql = "select length('aaa');";
        assertPlanContains(sql, "char_length('aaa')");

        sql = "SELECT str_to_map('a:1|b:2|c:3', '|', ':');";
        assertPlanContains(sql, "str_to_map(split('a:1|b:2|c:3', '|'), ':')");

        sql = "SELECT str_to_map('a');";
        assertPlanContains(sql, "str_to_map(split('a', ','), ':')");

        sql = "SELECT str_to_map('a:1|b:2|c:3', '|');";
        assertPlanContains(sql, "str_to_map(split('a:1|b:2|c:3', '|'), ':')");

        sql = "SELECT str_to_map('a:1,b:2,c:null');";
        assertPlanContains(sql, "str_to_map(split('a:1,b:2,c:null', ','), ':')");

        sql = "SELECT replace('hello-world', '-');";
        assertPlanContains(sql, "'helloworld'");

        sql = "SELECT replace('hello-world', '-', '$');";
        assertPlanContains(sql, "'hello$world'");

        sql = "select index('hello', 'l')";
        assertPlanContains(sql, "instr('hello', 'l')");
    }

    @Test
    public void testURLFnTransform() throws Exception {
        String sql = "select url_extract_path('https://www.starrocks.io/showcase?query=1')";
        assertPlanContains(sql, "parse_url('https://www.starrocks.io/showcase?query=1', 'PATH')");
    }

    @Test
    public void testJsonFnTransform() throws Exception {
        String sql = "select json_array_length('[1, 2, 3]')";
        assertPlanContains(sql, "json_length(CAST('[1, 2, 3]' AS JSON))");

        sql = "select json_parse('{\"a\": {\"b\": 1}}');";
        assertPlanContains(sql, "parse_json('{\"a\": {\"b\": 1}}')");

        sql = "select json_extract(json_parse('{\"a\": {\"b\": 1}}'), '$.a.b')";
        assertPlanContains(sql, "get_json_string(parse_json('{\"a\": {\"b\": 1}}'), '$.a.b')");

        sql = "select json_extract(JSON '{\"a\": {\"b\": 1}}', '$.a.b');";
        assertPlanContains(sql, "get_json_string('{\"a\": {\"b\": 1}}', '$.a.b')");

        sql = "select json_format(JSON '[1, 2, 3]')";
        assertPlanContains(sql, "'[1, 2, 3]'");

        sql = "select json_format(json_parse('{\"a\": {\"b\": 1}}'))";
        assertPlanContains(sql, "CAST(parse_json('{\"a\": {\"b\": 1}}') AS VARCHAR)");

        sql = "select json_size('{\"x\": {\"a\": 1, \"b\": 2}}', '$.x');";
        assertPlanContains(sql, "json_length(CAST('{\"x\": {\"a\": 1, \"b\": 2}}' AS JSON), '$.x')");

        sql = "select json_extract_scalar('[1, 2, 3]', '$[2]');";
        assertPlanContains(sql, "CAST(json_query(CAST('[1, 2, 3]' AS JSON), '$[2]') AS VARCHAR)");

        sql = "select json_extract_scalar(JSON '{\"a\": {\"b\": 1}}', '$.a.b');";
        assertPlanContains(sql, "CAST(json_query(CAST('{\"a\": {\"b\": 1}}' AS JSON), '$.a.b') AS VARCHAR)");

        sql = "select json_extract_scalar(json_parse('{\"a\": {\"b\": 1}}'), '$.a.b');";
        assertPlanContains(sql, "CAST(json_query(parse_json('{\"a\": {\"b\": 1}}'), '$.a.b') AS VARCHAR)");
    }

    @Test
    public void testExtractFnTransform() throws Exception {
        String sql = "SELECT extract(dow FROM TIMESTAMP '2022-10-20 05:10:00')";
        assertPlanContains(sql, "dayofweek_iso('2022-10-20 05:10:00')");
        sql = "SELECT extract(week FROM TIMESTAMP '2022-10-20 05:10:00')";
        assertPlanContains(sql, "week_iso('2022-10-20 05:10:00')");
    }


    @Test
    public void testBitFnTransform() throws Exception {
        String sql = "select bitwise_and(19,25)";
        assertPlanContains(sql, "17");

        sql = "select bitwise_not(19)";
        assertPlanContains(sql, "~ 19");

        sql = "select bitwise_or(19,25)";
        assertPlanContains(sql, "27");

        sql = "select bitwise_xor(19,25)";
        assertPlanContains(sql, "10");

        sql = "select bitwise_left_shift(1, 2)";
        assertPlanContains(sql, "1 BITSHIFTLEFT 2");

        sql = "select bitwise_right_shift(8, 3)";
        assertPlanContains(sql, "8 BITSHIFTRIGHT 3");
    }

    @Test
    public void testMathFnTransform() throws Exception {
        String sql = "select truncate(19.25)";
        assertPlanContains(sql, "truncate(19.25, 0)");
    }

    @Test
    public void testUnicodeFnTransform() throws Exception {
        String sql = "select to_utf8('123')";
        assertPlanContains(sql, "to_binary('123', 'utf8')");

        sql = "select from_utf8(to_utf8('123'))";
        assertPlanContains(sql, "from_binary(to_binary('123', 'utf8'), 'utf8')");
    }

    @Test
    public void testBinaryFunction() throws Exception {
        String sql = "select x'0012'";
        assertPlanContains(sql, "'0012'");

        sql = "select md5(x'0012');";
        assertPlanContains(sql, "md5(from_binary('0012', 'utf8'))");

        sql = "select md5(tk) from tall";
        assertPlanContains(sql, "md5(from_binary(11: tk, 'utf8'))");

        sql = "select to_hex(tk) from tall";
        assertPlanContains(sql, "hex(11: tk)");

        sql = "select sha256(x'aaaa');";
        assertPlanContains(sql, "sha2(from_binary('AAAA', 'utf8'), 256)");

        sql = "select sha256(tk) from tall";
        assertPlanContains(sql, "sha2(from_binary(11: tk, 'utf8'), 256)");
        System.out.println(getFragmentPlan(sql));

        sql = "select from_hex(hex('starrocks'))";
        assertPlanContains(sql, "hex_decode_binary(hex('starrocks'))");
    }

    @Test
    public void testInformationFunction() throws Exception {
        String sql = "select connection_id() from tall";
        assertPlanContains(sql, "<slot 12> : 0");

        sql = "select catalog() from tall";
        assertPlanContains(sql, "<slot 12> : 'default_catalog'");

        sql = "select database() from tall";
        assertPlanContains(sql, "<slot 12> : 'test'");

        sql = "select schema() from tall";
        assertPlanContains(sql, "<slot 12> : 'test'");

        sql = "select user() from tall";
        assertPlanContains(sql, "<slot 12> : '\\'root\\'@%'");

        sql = "select CURRENT_USER from tall";
        assertPlanContains(sql, "<slot 12> : '\\'root\\'@\\'%\\''");

        sql = "select CURRENT_ROLE from tall";
        assertPlanContains(sql, "<slot 12> : 'root'");
    }

    @Test
    public void testIsNullFunction() throws Exception {
        String sql = "select isnull(1)";
        assertPlanContains(sql, "<slot 2> : FALSE");

        sql = "select isnull('aaa')";
        assertPlanContains(sql, "<slot 2> : FALSE");

        sql = "select isnull(null)";
        assertPlanContains(sql, "<slot 2> : TRUE");

        sql = "select isnull(1, 2)";
        analyzeFail(sql, "isnull function must have 1 argument");

        sql = "select isnotnull(1)";
        assertPlanContains(sql, "<slot 2> : TRUE");

        sql = "select isnotnull('aaa')";
        assertPlanContains(sql, "<slot 2> : TRUE");

        sql = "select isnotnull(null)";
        assertPlanContains(sql, "<slot 2> : FALSE");

        sql = "select isnotnull(1, 2)";
        analyzeFail(sql, "isnotnull function must have 1 argument");
    }

    @Test
    public void testUtilityFunction() throws Exception {
        String sql = "select current_catalog";
        assertPlanContains(sql, "<slot 2> : 'default_catalog'");

        sql = "select current_schema";
        assertPlanContains(sql, "<slot 2> : 'test'");
    }


    @Test
    public void testHllFunction() throws Exception {
        String sql = "select empty_approx_set()";
        assertPlanContains(sql, "<slot 2> : HLL_EMPTY()");

        sql = "select approx_set(\"tc\") from tall";
        assertPlanContains(sql, "<slot 12> : hll_hash(CAST(3: tc AS VARCHAR))");

        sql = "select merge(approx_set(\"tc\")) from tall";
        assertPlanContains(sql, "hll_raw_agg(hll_hash(CAST(3: tc AS VARCHAR)))");
    }
}
