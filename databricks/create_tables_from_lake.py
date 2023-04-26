# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, ByteType, ShortType, VarcharType, BooleanType, TimestampType,LongType
from pyspark.sql.functions import col, to_timestamp, expr
from datetime import datetime

# COMMAND ----------

spark

# COMMAND ----------

storage_account_name = "REDACTED"
storage_account_access_key = "REDACTED"

spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
spark.conf.set(f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net',storage_account_access_key)



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Accounts DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read, Parse, and Clean Data Types

# COMMAND ----------

# Intended Columns
#         StructField("leagueId", StringType(), True),
#         StructField("name", StringType(), True),
#         StructField("region", StringType(), True),
#         StructField("region_group", StringType(), True),
#         StructField("profileIconId", ShortType(), True),
#         StructField("summonerLevel", ShortType(), True),
#         StructField("leaguePoints", ShortType(), True),
#         StructField("rank", StringType(), True),
#         StructField("wins", ShortType(), True),
#         StructField("losses", ShortType(), True),
#         StructField("veteran", BooleanType(), True),
#         StructField("inactive", BooleanType(), True),
#         StructField("freshBlood", BooleanType(), True),
#         StructField("hotStreak", BooleanType(), True),
#         StructField("revisionDate", TimestampType(), True),
#         StructField("last_updated", TimestampType(), True),

accounts_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/accounts/*.csv")

accounts_df = accounts_df.withColumn("revisionDate", to_timestamp(col("revisionDate"))) \
                .withColumn("profileIconId", col("profileIconId").cast(ShortType())) \
                .withColumn("summonerLevel", col("summonerLevel").cast(ShortType())) \
                .withColumn("leaguePoints", col("leaguePoints").cast(ShortType())) \
                .withColumn("wins", col("wins").cast(ShortType())) \
                .withColumn("losses", col("losses").cast(ShortType()))

accounts_df.schema 



# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster and Partitioning

# COMMAND ----------

# region_groups_count =accounts_df.select("region_group").distinct().count()

spark.sql("DROP TABLE IF EXISTS accounts")
accounts_df.write.partitionBy("leagueId").mode("overwrite").saveAsTable("accounts")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Test
# MAGIC Uncomment the code below if you want to test performance

# COMMAND ----------


# spark.sql("DROP TABLE IF EXISTS accounts_not_clustered")

# accounts_df.write.mode("overwrite").saveAsTable("accounts_not_clustered")

# start = datetime.now()
# test = spark.sql("SELECT COUNT(*) FROM accounts GROUP BY leagueId").show(1)
# print(f"Took {datetime.now() - start} to complete with clustered dataframe")

# start = datetime.now()
# test = spark.sql("SELECT COUNT(*) FROM accounts_not_clustered GROUP BY leagueId").show(1)

# print(f"Took {datetime.now() - start } to complete with non paritioned dataframe")

# print("Partitions can be at least 25 percent faster.  Similar steps can be reproduced in the following data frame.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Champion Mastery DataFrame

# COMMAND ----------

#         StructField("summonerName", StringType(), True),
#         StructField("region", StringType(), True),
#         StructField("championId", LongType(), True),
#         StructField("championLevel", LongType(), True),
#         StructField("championPoints", LongType(), True),
#         StructField("lastPlayTime", LongType(), True),
#         StructField("championPointsSinceLastLevel", LongType(), True),
#         StructField("championPointsUntilNextLevel", LongType(), True),
#         StructField("chestGranted", BooleanType(), True),
#         StructField("tokensEarned", LongType(), True),

champion_mastery_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/champion_mastery/*.csv")

champion_mastery_df = champion_mastery_df.withColumn("lastPlayTime", to_timestamp(col("lastPlayTime"))) \
                        .withColumn("championId", col("championId").cast(ShortType())) \
                        .withColumn("championLevel", col("championLevel").cast(ByteType())) \
                        .withColumn("tokensEarned", col("tokensEarned").cast(ByteType())) \

champion_mastery_df.printSchema()
spark.sql("DROP TABLE IF EXISTS champion_mastery")
champion_mastery_df.write.partitionBy("championId").mode("overwrite").saveAsTable("champion_mastery")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leagues DataFrame

# COMMAND ----------

leagues_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/leagues/*/*/*/*.csv")
leagues_df.drop_duplicates(["tier","division"]).show()
leagues_df.printSchema()
                
spark.sql("DROP TABLE IF EXISTS leagues")
leagues_df.write.partitionBy("tier").mode("overwrite").saveAsTable("leagues")
            

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Dragon DataFrames

# COMMAND ----------

champion_infos_df = spark.read.format("parquet").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/data_dragon/13.7.1/champion_infos.parquet")
champion_infos_df = champion_infos_df \
                        .withColumn("key", col("key").cast(ShortType())) \
                        .withColumn("attack_stat", col("attack_stat").cast(ShortType())) \
                        .withColumn("defense_stat", col("defense_stat").cast(ShortType())) \
                        .withColumn("magic_stat", col("magic_stat").cast(ShortType())) \
                        .withColumn("difficulty_stat", col("difficulty_stat").cast(ShortType())) \
                        .withColumn("hpperlevel", col("hpperlevel").cast(ShortType())) \
                        .withColumn("base_mp", col("base_mp").cast(ShortType())) \
                        .withColumn("base_armor", col("base_armor").cast(ShortType())) \
                        .withColumn("base_spellblock", col("base_spellblock").cast(ShortType())) \
                        .withColumn("base_attackrange", col("base_attackrange").cast(ShortType())) \
                        .withColumn("base_crit", col("base_crit").cast(ShortType())) \
                        .withColumn("critperlevel", col("critperlevel").cast(ShortType())) \
                        .withColumn("base_attackdamage", col("base_attackdamage").cast(ShortType())) \

champion_infos_df.printSchema()
spark.sql("DROP TABLE IF EXISTS champion_info")
champion_infos_df.write.partitionBy("primary_class").mode("overwrite").saveAsTable("champion_info")

# COMMAND ----------

item_infos_df = spark.read.format("parquet").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/data_dragon/13.7.1/item_infos.parquet")
item_infos_df = item_infos_df.withColumn("id", col("id").cast(ShortType()))\
    .withColumn("baseGold", col("baseGold").cast(ShortType()))\
    .withColumn("sellGold", col("sellGold").cast(ShortType()))\
    .withColumn("totalGold", col("totalGold").cast(ShortType()))\
    .withColumn("maximumStacks", col("maximumStacks").cast(ShortType()))\
    .withColumn("depth", col("depth").cast(ShortType()))\
    .withColumn("specialRecipe", col("specialRecipe").cast(ShortType()))

item_infos_df.printSchema()

spark.sql("DROP TABLE IF EXISTS item_info")
item_infos_df.write.mode("overwrite").saveAsTable("item_info")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS map_info")
map_infos_df = spark.read.format("parquet").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/data_dragon/13.7.1/map_infos.parquet") \
    .write.mode("overwrite").saveAsTable("map_info")


# COMMAND ----------

rune_infos_df = spark.read.format("parquet").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/data_dragon/13.7.1/rune_infos.parquet")
rune_infos_df = rune_infos_df.withColumn("id", col("id").cast(ShortType()))\
             .withColumn("rune_level", col("rune_level").cast(ShortType()))\

spark.sql("DROP TABLE IF EXISTS rune_info")
rune_infos_df.write.mode("overwrite").saveAsTable("rune_info")

# COMMAND ----------

summoner_spells_info_df = spark.read.format("parquet").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/data_dragon/13.7.1/summoner_spells_info.parquet")
summoner_spells_info_df = summoner_spells_info_df.withColumn("key", col("key").cast(ShortType()))\
                        .withColumn("maxrank", col("maxrank").cast(ShortType()))\
                        .withColumn("cooldown", col("cooldown").cast(ShortType()))\
                        .withColumn("cooldownBurn", col("cooldownBurn").cast(ShortType()))\
                        .withColumn("cost", col("cost").cast(ShortType()))\
                        .withColumn("minimumSummonerLevel", col("minimumSummonerLevel").cast(ShortType()))\
                        .withColumn("rangeBurn", col("rangeBurn").cast(ShortType()))\

spark.sql("DROP TABLE IF EXISTS summoner_spells_info")
summoner_spells_info_df.write.mode("overwrite").saveAsTable("summoner_spells_info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## General Matches DataFrame 

# COMMAND ----------

general_match_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/match/general/*/*/*/*.csv")

general_match_df = general_match_df.withColumn("gameCreation", to_timestamp(col("gameCreation")/1000)) \
            .withColumn("gameStartTimestamp", to_timestamp(col("gameStartTimestamp")/1000)) \
            .withColumn("gameEndTimestamp", to_timestamp(col("gameEndTimestamp")/1000)) \
            .withColumn("queueId", col("queueId").cast(ShortType()))\
            .withColumn("mapId", col("mapId").cast(ShortType()))\
            .withColumn("dataVersion", col("dataVersion").cast(ByteType()))\
            .withColumn("gameDuration", expr("interval '1 second' * gameDuration"))

spark.sql("DROP TABLE IF EXISTS general_match")
general_match_df.write.partitionBy("gameMode").mode("overwrite").saveAsTable("general_match")


# COMMAND ----------

players_match_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/match/players/*/*/*/*.csv")

for column, col_type in players_match_df.dtypes:
    # To Fix, challenges df not removed in ETL process: match_entries.py:getMatchPlayersInfo
    if "challenges" in column:
        players_match_df = players_match_df.drop(column)
    if col_type == "bigint":
        players_match_df = players_match_df.withColumn(column, col(column).cast(ShortType()))

spark.sql("DROP TABLE IF EXISTS players_match")
players_match_df.write.partitionBy("championId").mode("overwrite").saveAsTable("players_match")

# COMMAND ----------

spark.conf.set("spark.sql.parquet.enableVectorizedReader","true")
players_challenges_match_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/match/players_challenges/*/*/*/*.csv")
print("dumaa")
double_rows = ["damagePerMinute",
"damageTakenOnTeamPercentage",
"effectiveHealAndShielding",
"gameLength",
"goldPerMinute",
"kda",
"killParticipation'",
"shortestTimeToAceFromFirstTakedown",
"teamDamagePercentage"]


for column, col_type in players_challenges_match_df.dtypes:
    if (col_type == "bigint" or col_type =="double") and col_type not in double_rows:
        # print("gumna siya", column, col_type)
        players_challenges_match_df = players_challenges_match_df.withColumn(column, col(column).cast(ShortType()))

players_challenges_match_df = players_challenges_match_df.withColumn('alliedJungleMonsterKills', col('alliedJungleMonsterKills').cast(LongType()))
# print(players_challenges_match_df.dtypes)

#Table has no partitions

spark.sql("DROP TABLE IF EXISTS player_challenges_match")
players_challenges_match_df.write.mode("overwrite").saveAsTable("player_challenges_match")
# spark.conf.set("spark.sql.parquet.enableVectorizedReader","true")


# COMMAND ----------

teams_match_df = spark.read.format("csv").option("header","true").load(f"wasbs://REDACTED@REDACTED.blob.core.windows.net/resources/match/teams/*/*/*/*.csv")

for column, col_type in teams_match_df.dtypes:
    if (col_type == "bigint" or col_type =="double"):
        teams_match_df = teams_match_df.withColumn(column, col(column).cast(ShortType()))

spark.sql("DROP TABLE IF EXISTS teams_match")
#Table has no partitions
teams_match_df.write.mode("overwrite").saveAsTable("teams_match")


# COMMAND ----------

spark.sql("SELECT matchId, championId FROM players_match ORDER BY championId DESC").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC      SELECT COUNT(DISTINCT(matchId)) FROM `dbt_petecastle`.`champion_picks_bans`
# MAGIC     WHERE type = "ban" AND championId = "Aatrox"

# COMMAND ----------

spark.sql(
    """
    WITH temp_table AS (
        SELECT * FROM dbt_petecastle.champion_item_picks
        WHERE championId = 235 
        -- GROUP BY matchId
    )
    SELECT *
    FROM temp_table
    ORDER BY matchId
    """).show()

# -- 3262


# COMMAND ----------

spark.sql(
    """
        SELECT 
            primaryStyle_0_id,
            -- primaryStyle_0_var1,
            -- primaryStyle_0_var2,
            -- primaryStyle_0_var3,
            primaryStyle_1_id,
            -- primaryStyle_1_var1,
            -- primaryStyle_1_var2,
            -- primaryStyle_1_var3,
            primaryStyle_2_id,
            -- primaryStyle_2_var1,
            -- primaryStyle_2_var2,
            -- primaryStyle_2_var3,
            primaryStyle_3_id
            -- primaryStyle_3_var1,
            -- primaryStyle_3_var2,
            -- primaryStyle_3_var3
        FROM players_match
    
    """).show()
