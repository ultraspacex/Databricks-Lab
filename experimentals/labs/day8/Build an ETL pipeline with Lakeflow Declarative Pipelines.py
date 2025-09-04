# Databricks notebook source
# MAGIC %sql
# MAGIC -- Which artists released the most songs each year in 1990 or later?
# MAGIC SELECT artist_name, total_number_of_songs, year
# MAGIC FROM catalog_yutthanal.lab8.top_artists_by_year
# MAGIC WHERE year >= 1990
# MAGIC ORDER BY total_number_of_songs DESC, year DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find songs with a 4/4 beat and danceable tempo
# MAGIC  SELECT artist_name, song_title, tempo
# MAGIC  FROM catalog_yutthanal.lab8.songs_prepared
# MAGIC  WHERE time_signature = 4 AND tempo between 100 and 140;
