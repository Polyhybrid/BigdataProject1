from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# 1. สร้าง SparkSession (จำเป็นต้องมีทุกครั้งในสคริปต์ใหม่)
spark = SparkSession.builder.appName("Spotify_Dance").getOrCreate()

# 2. โหลดข้อมูลเข้าตัวแปร df (ใช้ option ป้องกันคอลัมน์เลื่อนเหมือนเดิมครับ)
df = spark.read.csv(
    "SpotifyFeatures.csv", 
    header=True, 
    inferSchema=True,
    quote='"',
    escape='"',
    mode="DROPMALFORMED"
)

# 3. กรองเฉพาะเพลงที่มี Danceability สูงมาก (> 0.8) 
# และเลือกดูค่าคุณลักษณะที่น่าสนใจ
dance_songs = df.filter(col("danceability") > 0.8) \
    .select("track_name", "artist_name", "genre", "danceability", "energy", "valence") \
    .orderBy(desc("danceability")) \
    .limit(5)

# แสดงผลลัพธ์
dance_songs.show()