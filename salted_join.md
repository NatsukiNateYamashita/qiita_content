# pyspark における効率的なjoin操作

- 問題：pysparkのjoinが非常に遅かったりメモリ不足で処理ストップしたり分析が滞ってしまった
- 原因：data skew や アンバランスなパーティション
- 解決方法：broadcast, salting, バランスよくパーティションする

下部のreferenceに記載されている記事を参考に、pyspark における効率的な join (left, innerのみ対応) を実装しました。

[github: NatsukiNateYamashita/efficient_join - 効率的な PySpark join](https://github.com/NatsukiNateYamashita/efficient_join)


## 概要

このモジュールは、Apache Sparkでデータ処理を行う際によく発生するdata skew問題を軽減するためのユーティリティ関数を提供します。特に大規模SparkDataFrameと小規模SparkDataFrameの`join`処理でパフォーマンスを向上させる機能を含んでいます。

## 機能

- **salted join**: 大規模SparkDataFrameと小規模SparkDataFrameをjoinする際のdata skewを軽減します
- **cross join**: `broadcast`変数を活用した効率的なcross joinを実行します

## salted_join() のパフォーマンス改善効果

| joinタイプ | SparkDataFrameサイズ | built-in join() | salted_join() | 改善率 |
|-----------|---------------|--------|-------|-------------|
| **inner join** | Df_big: 約4億件<br>Df_small: 3.5万件 | 4分20秒 | 1分53秒 | **56.5%** |
| **inner join** | Df_big: 約4億件<br>Df_small: 1万件 | 3分2秒 | 1分31秒 | **50.0%** |
| **left join**  | Df_big: 約4億件<br>Df_small: 3.5万件 | 19.14秒 | 15.31秒 | **15.6%** |
| **left join**  | Df_big: 約4億件<br>Df_small: 1万件 | 7.98秒 | 6.39秒 | **20.0%** |

- **inner join**: 約50～56%の性能改善
- **left join**: 約15～20%の性能改善

## 主な関数

### `salted_join(df_big, big_key_column, df_small, small_key_column, join_type)`

大規模SparkDataFrameのキーに「salt」を追加し、小規模SparkDataFrameを複製することでdata skewを軽減します。

**引数:**
- `df_big`: 大規模SparkSparkDataFrame
- `big_key_column`: 大規模SparkDataFrameのjoinキー列名
- `df_small`: 小規模SparkSparkDataFrame
- `small_key_column`: 小規模SparkDataFrameのjoinキー列名
- `join_type`: joinタイプ（「inner」、「left」、「leftouter」、「left_outer」、「leftanti」、「left_anti」のいずれか）

**戻り値:**
- joinされたSparkSparkDataFrame

**利点:**
- **data skew問題の解決**: パーティション間のデータ分布の偏りによる処理のボトルネックを防止します
- **分散コンピューティング効率の向上**: ワーカーノード間でデータを均等に分散させることで、並列処理を最適化します
- **エグゼキューターの障害防止**: skewしたキーを処理する特定のエグゼキューターでのメモリ不足エラーのリスクを軽減します
- **処理時間の短縮**: バランスの取れたワークロードにより、全体的なジョブ完了が高速化します

**制限事項:**
- メモリエラーの可能性があるため、使用可能なjoinタイプは限定されています

### `crossjoin(df_big, df_small)`

大規模SparkDataFrameと小規模SparkDataFrameのcross joinを、小規模SparkDataFrameを`broadcast`して効率的に実行します。

**引数:**
- `df_big`: 大規模SparkSparkDataFrame
- `df_small`: 小規模SparkSparkDataFrame（`broadcast`されます）

**戻り値:**
- cross joinされたSparkSparkDataFrame

## salted joinの仕組み

このユーティリティは以下の手法でdata skew問題に対処します：

1. **キーのsalt化**: 大規模SparkDataFrameのキーにランダムな接尾辞（0-2）を追加し、各キーに対して複数の仮想パーティションを効果的に作成します
2. **小規模SparkDataFrameの展開**: 小規模SparkDataFrameの各行を対応する接尾辞で3回複製し、salt化されたキーとマッチングできるようにします
3. **バランスの取れた分散**: 高頻度キーのすべてのレコードを1つのパーティションで処理する代わりに、作業を複数のパーティションに分散します

変換プロセス:
- 元のjoin条件: `df_big.key = df_small.key`
- 変換後のjoin条件: `df_big.key_salted = concat(df_small.key, "_", exploded_value)`

このアプローチにより、Sparkクラスター内のエグゼキューターノード間でワークロードが均等に分散され、並列処理効率が大幅に向上します。

## 使用例

```python
from spark_utils import salted_join, crossjoin

# salted joinの使用例
result_df = salted_join(
    large_customer_df, 
    "customer_id", 
    small_product_df, 
    "customer_id", 
    "left"
)

# cross joinの使用例
result_cross_df = crossjoin(transaction_df, calendar_df)
```

## パフォーマンスへの影響

salted joinアプローチには以下の影響があります：

- **プラス面**: skewしたキー分布を持つSparkDataFrameで大幅なパフォーマンス向上
- **プラス面**: クラスター全体でのリソース使用率の向上
- **考慮点**: 前処理（saltの追加と小さいSparkDataFrameの展開）による若干のオーバーヘッド
- **考慮点**: saltの数（現在は3に設定）はskewの深刻度に基づいて調整が必要な場合があります

## 参考文献

このユーティリティの開発には以下の文献を参考にしました：

- [Why Your Spark Apps Are Slow Or Failing, Part II: Data Skew and Garbage Collection](https://dzone.com/articles/why-your-spark-apps-are-slow-or-failing-part-ii-da)
- [Spark's Salting — A Step Towards Mitigating Skew Problem](https://medium.com/curious-data-catalog/sparks-salting-a-step-towards-mitigating-skew-problem-5b2e66791620)

## 注記
本記事、および、[github: efficient_join/README.md](https://github.com/NatsukiNateYamashita/efficient_join/blob/main/README.md)は、コードと参考文献に基づいてClaude 3.7 Sonnetによって生成されました
