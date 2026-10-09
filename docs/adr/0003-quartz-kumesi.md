# ADR 0003 — Quartz.NET kümesi ve idempotent işler

**Durum:** Kabul edildi

- Zamanlayıcı: Quartz.NET, PostgreSQL job store (`qrtz_*` tabloları), kümeleme açık. Tetikleyici her küme turunda bir düğümde çalışır; düğüm ölürse diğeri devralır
  (test: iki düğümle 6 sn'de ~6 çalıştırma, bir düğüm durdurulunca kalan düğüm sürdürür).
- Küme, çift çalışmayı **azaltır, garanti etmez** (örneğin devralma sırasında bir tur iki kez çalışabilir). Bu yüzden her iş idempotent:
  - Rollup: aralığı baştan hesaplar, `ON CONFLICT DO UPDATE` ile yazar. Artımlı toplama olsaydı çift çalışma sayıyı bozardı.
  - Uyarı: `UNIQUE (cihaz, metrik, kova, tür)` + `DO NOTHING`.
  - Bakım: `CREATE ... IF NOT EXISTS`, `DROP ... IF EXISTS`.
- İşler `[DisallowConcurrentExecution]`: aynı iş aynı kümede üst üste binmez.
- Rollup yalnızca sınırlı bir geriye dönük pencereyi (`RollupLookbackHours`) yeniden hesaplar: maliyet sınırlı, geç veri o pencere içinde kendiliğinden düzelir.
  Pencere dışı geç veri elle geri doldurulmalı (sınır olarak belgelendi).
- Zamanlayıcı kümesi ile veri alımı bağımsız: bir düğüm yalnızca işleri, bir diğeri yalnızca alımı çalıştırabilir.
