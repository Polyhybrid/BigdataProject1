from pyspark.sql import SparkSession

# 1. สร้าง SparkSession (จำเป็นต้องมีทุกครั้ง)
spark = SparkSession.builder.appName("Spotify_Pop90_SQL").getOrCreate()

# 2. โหลดข้อมูลเข้าตัวแปร df (ใช้ option ป้องกันคอลัมน์เลื่อนเหมือนเดิมครับ)
df = spark.read.csv(
    "SpotifyFeatures.csv", 
    header=True, 
    inferSchema=True,
    quote='"',
    escape='"',
    mode="DROPMALFORMED"
)

# 3. สร้าง Temporary View เพื่อให้เขียน SQL ได้
df.createOrReplaceTempView("spotify_view")

# 4. เขียนคำสั่ง SQL 
sql_query = """
    SELECT artist_name, COUNT(track_name) as hit_songs_count
    FROM spotify_view
    WHERE popularity >= 90
    GROUP BY artist_name
    ORDER BY hit_songs_count DESC
    LIMIT 5
"""

# 5. รัน Query และแสดงผล
top_artists_sql = spark.sql(sql_query)
print("Top Artists with Most Hit Songs (Popularity >= 90):")
top_artists_sql.show()