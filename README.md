# WeatherWhispers Project: Data Engineering for Weather and Twitter Correlation

## 👥 Anggota Tim
- **Barbara Neanake A.** (22/494495/TK/54238)
- **Nasywa Rahmadhani P.S.** (22/498375/TK/54665)
- **Rizkita Alisha R.** (22/494942/TK/54347)

## 📜 Project Overview
WeatherWhispers adalah proyek data engineering yang bertujuan untuk menganalisis hubungan antara data cuaca dan aktivitas media sosial di Yogyakarta, khususnya dari Twitter. Kami menggunakan pipeline data end-to-end untuk mengumpulkan, memproses, dan menganalisis data guna mendapatkan wawasan dan prediksi mengenai bagaimana kondisi cuaca memengaruhi postingan di Twitter.

## 🧐 Latar Belakang
Mahasiswa di Yogyakarta sangat bergantung pada media sosial untuk mengetahui cuaca, terutama saat menghadapi hujan deras atau panas terik yang dapat memengaruhi aktivitas sehari-hari. Kami mengamati bahwa perubahan cuaca sering memicu banyaknya percakapan dan reaksi langsung di Twitter. Melalui proyek ini, kami ingin memahami pola tersebut dan memprediksi bagaimana cuaca memengaruhi tren media sosial di masa depan.

## 📊 Data Sources
1. **Twitter API**: Mengambil data tentang percakapan dan tren terkait cuaca.
2. **Virtual Crossing API**: Menyediakan data cuaca terkini, termasuk temperatur, curah hujan, dan prakiraan cuaca.

## 🔧 Arsitektur Teknologi
### End-to-End Data Pipeline
1. **Pengumpulan Data**: Menggunakan Apache Airflow untuk memicu pengambilan data dari Twitter API dan Virtual Crossing API secara berkala.
2. **Preprocessing & ETL**: Data diolah dan ditransformasikan sebelum disimpan di data warehouse (misalnya, Snowflake).
3. **Penyimpanan**: Data yang sudah diolah disimpan di data warehouse untuk keperluan analisis lebih lanjut.
4. **Modeling**: Google Colab digunakan untuk menjalankan algoritme predictive modeling, seperti forecasting, untuk memprediksi frekuensi konten terkait cuaca di Twitter.

## 🧠 Algoritme Predictive Modeling
Kami menggunakan pendekatan **forecasting** untuk memprediksi frekuensi postingan dengan kata kunci tertentu berdasarkan data cuaca. Model ini dioptimalkan dengan data historis yang telah dikumpulkan.

## 🎥 Demo & Presentasi
- **Video Presentasi**: [Tautan ke Video](#)
- **Blog Post**: [Tautan ke Blog](#)

## ✨ Hasil dan Wawasan
Proyek ini menunjukkan bagaimana cuaca ekstrem, seperti hujan deras atau suhu panas, dapat berdampak langsung pada jumlah dan jenis postingan di Twitter. Hasil ini memberikan wawasan yang berpotensi berguna untuk pengambilan keputusan terkait perencanaan cuaca dan komunikasi darurat.

## 📚 Referensi
- [Twitter API Documentation](https://developer.twitter.com)
- [Virtual Crossing API Documentation](https://www.virtualcrossing.com)

# 🔗 **LINK PENTING**
- **Repository GitHub**: [link]
- **Blog Post**: [link]
- **Video Presentasi**: [link]
