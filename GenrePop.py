from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, round

# 1. สร้าง SparkSession
spark = SparkSession.builder.appName("Spotify_Project_1").getOrCreate()

# 2. โหลดข้อมูล (เพิ่ม option ป้องกันคอลัมน์เลื่อนเนื่องจากเครื่องหมายลูกน้ำในชื่อเพลง)
df = spark.read.csv(
    "SpotifyFeatures.csv", 
    header=True, 
    inferSchema=True,
    quote='"',       # ระบุว่าข้อมูลอาจจะถูกครอบด้วย Double Quote
    escape='"',      # ให้มองข้ามลูกน้ำ (,) ที่อยู่ข้างใน Double Quote
    mode="DROPMALFORMED" # ถ้ามีแถวไหนคอลัมน์ยังพังอยู่ ให้ดรอปทิ้งไปเลย ไม่ต้องเอามาคิด
)

# 3. การวิเคราะห์: หา Top 10 แนวเพลงที่มีค่า Popularity เฉลี่ยสูงสุด
top_genres = df.groupBy("genre") \
    .agg(round(avg("popularity"), 2).alias("avg_popularity")) \
    .orderBy(desc("avg_popularity")) \
    .limit(10)

# แสดงผลลัพธ์
top_genres.show()