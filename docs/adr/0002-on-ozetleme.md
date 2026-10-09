# ADR 0002 — Ön özetleme ve yalnızca birleştirilebilir istatistikler

**Durum:** Kabul edildi

- Veritabanına ham olay yazmak yerine `(cihaz, metrik, dakika)` başına `count, sum, min, max, sumsq` yazılır. Bu beşlisi **birleştirilebilir**dir:
  iki özetin birleşimi, olayların birleşiminin özetine eşittir (testte rastgele bölmelerle doğrulanır). Sıra ve parti sınırı sonucu etkilemez, bu da geç/sırasız olayları ve
  yeniden oynatmayı basitleştirir.
- Ortalama = sum/count, varyans = sumsq/count − ortalama². Yüzdelikler birleştirilemediği için bilerek yok.
- Değerler tamsayı santi-birim: toplamlar kayan nokta hatası olmadan birebir, bu da "bağımsız hesapla birebir eşit" testini mümkün kılar.
  `sumsq` değer aralığı ve olay sayısı için `bigint` sınırında kalır (değer ≤ 100,00 → 10⁸ × olay sayısı).
- Aynı partide aynı anahtarı birleştirmek yazma sayısını ~29 kat azaltır (40M olay → 1,37M satır). Kazancın büyüklüğü olay yoğunluğuna bağlıdır.
- Satırlar upsert öncesi birincil anahtar sırasına dizilir; farklı partisyonlar zaten ayrık cihaz kümelerine yazar.
- Dakika kovası olay zamanından hesaplanır; yazma, kova hangi günün bölümündeyse oraya gider (bölümler gerektiğinde oluşturulur).
