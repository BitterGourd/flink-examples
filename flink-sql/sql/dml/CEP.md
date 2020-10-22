> 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html



### 1.基本语法

```sql
SELECT T.aid, T.bid, T.cid
FROM MyTable
    MATCH_RECOGNIZE (
      PARTITION BY userid
      ORDER BY proctime
      MEASURES
        A.id AS aid,
        B.id AS bid,
        C.id AS cid
      PATTERN (A B C)
      DEFINE
        A AS name = 'a',
        B AS name = 'b',
        C AS name = 'c'
    ) AS T
```

每个 `MATCH_RECOGNIZE` 查询都包含以下子句：

- [PARTITION BY](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#partitioning) - 定义表的逻辑分区；类似于 `GROUP BY` 操作。
- [ORDER BY](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#order-of-events) - 指定传入行的排序方式；这是必须的，因为模式依赖于顺序。
- [MEASURES](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#define--measures) - 定义子句的输出；类似于 `SELECT` 子句。
- [ONE ROW PER MATCH](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#output-mode) - 输出方式，定义每个匹配项应产生多少行。
- [AFTER MATCH SKIP](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#after-match-strategy) - 指定下一个匹配的开始位置；这也是控制单个事件可以属于多少个不同匹配项的方法。
- [PATTERN](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#defining-a-pattern) - 允许使用类似于 *正则表达式* 的语法构造搜索的模式。
- [DEFINE](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/match_recognize.html#define--measures) - 本部分定义了模式变量必须满足的条件。

**<font color=red>注意</font>** 目前，`MATCH_RECOGNIZE` 子句只能应用于[追加表](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/dynamic_tables.html#update-and-append-queries)。此外，它也总是生成一个追加表。



### 2.示例

定义一张表 Ticker，该表包含特定时间点的股票价格。

```sql
Ticker
     |-- symbol: String                           # 股票的代号
     |-- price: Long                              # 股票的价格
     |-- tax: Long                                # 股票应纳税额
     |-- rowtime: TimeIndicatorTypeInfo(rowtime)  # 更改这些值的时间点
```

为了简化，我们只考虑单个股票 `ACME` 的传入数据。Ticker 可以类似于下表，其中的行是连续追加的。

```sql
symbol         rowtime         price    tax
======  ====================  ======= =======
'ACME'  '01-Apr-11 10:00:00'   12      1
'ACME'  '01-Apr-11 10:00:01'   17      2
'ACME'  '01-Apr-11 10:00:02'   19      1
'ACME'  '01-Apr-11 10:00:03'   21      3
'ACME'  '01-Apr-11 10:00:04'   25      2
'ACME'  '01-Apr-11 10:00:05'   18      1
'ACME'  '01-Apr-11 10:00:06'   15      1
'ACME'  '01-Apr-11 10:00:07'   14      2
'ACME'  '01-Apr-11 10:00:08'   24      2
'ACME'  '01-Apr-11 10:00:09'   25      2
'ACME'  '01-Apr-11 10:00:10'   19      1
```

现在的任务是找出一个单一股票价格不断下降的时期。为此，可以编写如下查询：

```sql
SELECT *
FROM Ticker
    MATCH_RECOGNIZE (
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            START_ROW.rowtime AS start_tstamp,
            LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,
            LAST(PRICE_UP.rowtime) AS end_tstamp
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO LAST PRICE_UP
        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)
        DEFINE
            PRICE_DOWN AS
                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR
                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),
            PRICE_UP AS
                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)
    ) MR;
```

此查询将 `Ticker` 表按照 `symbol` 列进行分区并按照 `rowtime` 属性进行排序。

`PATTERN` 子句指定我们对以下模式感兴趣：该模式具有开始事件 `START_ROW`，然后是一个或多个 `PRICE_DOWN` 事件，并以 `PRICE_UP` 事件结束。如果可以找到这样的模式，如 `AFTER MATCH SKIP TO LAST` 子句所示，则从最后一个 `PRICE_UP` 事件开始寻找下一个模式匹配。

`DEFINE` 子句指定 `PRICE_DOWN` 和 `PRICE_UP` 事件需要满足的条件。尽管不存在 `START_ROW` 模式变量，但它具有一个始终被评估为 `TRUE` 隐式条件。

模式变量 `PRICE_DOWN` 定义为价格小于满足 `PRICE_DOWN` 条件的最后一行。对于初始情况或没有满足 `PRICE_DOWN` 条件的最后一行时，该行的价格应小于该模式中前一行（由 `START_ROW` 引用）的价格。

模式变量 `PRICE_UP` 定义为价格大于满足 `PRICE_DOWN` 条件的最后一行。

此查询为股票价格持续下跌的每个期间生成摘要行。

在查询的 `MEASURES` 子句部分定义确切的输出行信息。输出行数由 `ONE ROW PER MATCH` 输出方式定义。

```sql
 symbol       start_tstamp       bottom_tstamp         end_tstamp
=========  ==================  ==================  ==================
ACME       01-APR-11 10:00:04  01-APR-11 10:00:07  01-APR-11 10:00:08
```

该行结果描述了从 `01-APR-11 10:00:04` 开始的价格下跌期，在 `01-APR-11 10:00:07` 达到最低价格，到 `01-APR-11 10:00:08` 再次上涨。