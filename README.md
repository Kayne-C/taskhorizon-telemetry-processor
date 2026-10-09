# TaskHorizon — Yüksek Hacimli Telemetri ve Arka Plan İş İşleyicisi

Milyonlarca IoT/ERP telemetri olayını **tam olarak bir kez** işleyip PostgreSQL'de dakika/saat bazında özetleyen, zamanlanmış işleri
**Quartz.NET** (kümelenmiş, PostgreSQL destekli) ile yöneten bir Worker Service. .NET 10.

Tasarımın merkezindeki soru: işleyici ortada ölürse, iki düğüm aynı partisyona saldırırsa ya da bir commit yarım kalırsa özetler
yine de **birebir doğru** mu? Bu repo bunu, olayları bağımsız bir yoldan yeniden hesaplayıp veritabanıyla karşılaştırarak sınar.

## Neler yapıyor

| Alan | Yaklaşım | Kanıt |
|---|---|---|
| **Eşzamansız işleme** | Partisyon başına üretici/tüketici ayrımı: bir görev olayları okuyup bellekte özetler, sınırlı bir kanalla (2 parti) commit eden görevi besler. DB yazması sürerken sıradaki parti hazırlanır | Ölçüm: 8M olay 3,6 sn |
| **Ön özetleme** | Olaylar `(cihaz, metrik, dakika)` anahtarıyla bellekte birleşir; DB'ye olay değil özet gider (`count, sum, min, max, sumsq`) | 40M olay → 1,37M satır (~29 kat azalma) |
| **Tam bir kez etki** | Parti özetleri ve partisyon **checkpoint'i tek işlemde**: `UPDATE ... WHERE next_offset = @beklenen` (CAS) + upsert + commit. Çökünce checkpoint'ten yeniden okunur | Test: commit öncesi/sırası/sonrası %15'er rastgele çökme → özetler bağımsız hesapla **birebir eşit** |
| **Çok düğüm** | Partisyon sahipliği PostgreSQL advisory lock ile; düğüm ölünce kilit düşer, diğeri devralır. CAS, kilidini kaybeden eski yazıcıyı (zombie) engeller | Test: 2 düğüm 8 partisyonu paylaşır, `düğüm A olayları + düğüm B olayları = toplam` (çift sayım yok); düğüm ortada durdurulur, diğeri bitirir |
| **Geç/sırasız olaylar** | Dakika kovası olay zamanından; özetler birleştirilebilir (toplama, min/max), sıra önemsiz | Test: ±60 sn jitter, özet doğru |
| **Zamanlanmış işler** | Quartz.NET, PostgreSQL job store, **kümelenmiş**: saatlik rollup, uyarı üretimi, bakım. Hepsi idempotent | Test: 2 düğüm, 6 sn'de ~6 çalıştırma (12 değil); bir düğüm düşünce diğeri sürdürür |
| **Rollup** | Dakika → saat, `ON CONFLICT DO UPDATE` ile yeniden hesaplanır (artımlı değil), tekrar çalıştırmak sonucu değiştirmez | Test: saatler dakikaların toplamına eşit; geç gelen veri sonraki turda yansır |
| **Uyarılar** | Eşik aşımı `UNIQUE(cihaz, metrik, kova, tür)` ile bir kez | Test: iş 3 kez çalışsa da her ani yükseliş tek uyarı |
| **Bölümleme ve saklama** | `agg_minute` günlük aralık bölümlü; gerektiğinde oluşturulur (advisory lock ile yarışa karşı), süresi dolan bölüm `DROP` (satır silmek yok) | Test: eşzamanlı bölüm oluşturma, saklama süresi |

## Ölçüm

AMD Ryzen 7 9800X3D (8 çekirdek/16 iş parçacığı), Windows, PostgreSQL 17 **Docker içinde, varsayılan ayarlarla** (aynı makine), Release derleme,
8 partisyon × cihaz başına 500, parti 50.000, aynı anda en çok 4 commit:

| Veri | Süre | Verim | Ort. commit | Çıktı |
|---|---|---|---|---|
| 8M olay | 3,6 sn | 2,19M olay/sn | 173 ms | 303 bin dakika satırı |
| 40M olay | 16,7 sn | 2,39M olay/sn | 163 ms | 1,37M satır, 472 MB (indeksler dahil), toplam sayı tam 40.000.000 |

Aynı veri üzerinde: saatlik rollup 1,37M satırı 48 bin saat satırına **1,3 sn**'de indirgiyor; uyarı taraması 117 ms.

**Bu sayıları doğru okumak için:** olay üreticisi ucuz ve aynı süreçte (bir mesaj kuyruğundan ağ üzerinden okumak ve JSON/protobuf ayrıştırmak bu rakamı belirgin düşürür);
PostgreSQL ve uygulama aynı makinede ve CPU paylaşıyor; tek veritabanı örneği, tek düğüm. Rakam "bu tasarımda DB tarafı darboğaz değil, ön özetleme işe yarıyor"
demektir, "üretimde saniyede 2 milyon olay" demek değildir.

## Hızlı başlangıç

```bash
docker compose up -d --build        # PostgreSQL + 2 worker düğümü (aynı veritabanı, aynı Quartz kümesi)
curl localhost:5401/stats           # ilerleme, olay/sn, partisyon checkpoint'leri
curl "localhost:5401/devices/3/minutes?metric=0&limit=5"
curl localhost:5401/alerts
```

Docker'sız karşılaştırma ölçümü: `dotnet run --project src/TaskHorizon.Worker -c Release -- --bench` (bir PostgreSQL bağlantısı gerekir,
`ConnectionStrings__Db` ortam değişkeniyle verilir). Tüm olaylar işlenince verim özetini yazıp çıkar.

Değerler tamsayı **santi-birim**dir (45,00 → 4500). Toplamların ve kareler toplamının (varyans için) kayan nokta hatasıyla değil
birebir doğru olması bunun sayesindedir.

## Mimari

```
Kaynak (partisyon p, offset o) ── okuma+özetleme görevi ─▶ [kanal, 2 parti] ─▶ commit görevi ─▶ PostgreSQL
   saf fonksiyon: aynı (p,o) → aynı olay                                          tek işlem:
                                                                                  1) checkpoint CAS   (kilit/çit)
                                                                                  2) agg_minute upsert (birleştirerek)
                                                                                  3) commit
Quartz (kümelenmiş) ─▶ RollupJob · AlertJob · MaintenanceJob   (hepsi idempotent)
```

- Bir cihaz her zaman aynı partisyondadır (anahtara göre bölümleme, Kafka'daki gibi). Bu yüzden iki partisyon aynı satırı güncellemez, kilit çekişmesi/deadlock olmaz.
  Satırlar birincil anahtar sırasıyla yazılır.
- Kararlar: [docs/adr](docs/adr).

## Testler

```bash
dotnet test --solution TaskHorizon.slnx     # Docker gerekir (Testcontainers ile gerçek PostgreSQL 17)
```

25 test: kaynak determinizmi, özet birleştirme (rastgele bölmelerle özellik testi), commit atomikliği, çit (fencing), çökme enjeksiyonu,
iki düğüm / düğüm ölümü, bölümleme, rollup, uyarılar, bakım, Quartz kümesi. Art arda 3 tam koşu geçti.

**Testlerin bulduğu gerçek hata:** iki düğüm aynı anda ayağa kalkınca (biri veri alırken) şema kurulumu `CREATE ... IF NOT EXISTS` ile bile
tablolarda kilit alıyor, ve çalışan yazımlarla **deadlock** oluşuyordu. Çözüm: kurulum bir sürüm tablosuyla korunur, zaten kuruluysa hiçbir DDL çalışmaz;
ilk kurulum advisory lock ile tek düğümde yapılır.

## Bilinen sınırlar (dürüst liste)

- Kaynak, **tekrar oynatılabilir sentetik bir günlük** (partisyon + offset ile saf fonksiyon). "Tam bir kez" garantisi tam olarak buna dayanır: kaynaktan aynı offset'ten tekrar okuyabilmek.
  Gerçek bir Kafka/MQTT/Event Hubs bağdaştırıcısı **yazılmadı**; offset'i olan bir aracıyla aynı model uygulanır, ama denenmedi.
- Yüzdelik dilimler (p95, p99) desteklenmez: birleştirilebilir değiller. Sayı/toplam/min/maks/kareler toplamı (ortalama ve standart sapma) var.
- Rollup yalnızca son `RollupLookbackHours` saatini yeniden hesaplar; daha eski geç veri için `RollupService.RunAllAsync` elle çağrılmalı (uç nokta yok).
- Saklama süresi duvar saatine değil **en yeni verinin zamanına** göre hesaplanır (sentetik veri geçmişte olduğu için).
- Partisyon sayısı ilk veriden sonra değiştirilemez: cihaz → partisyon eşlemesi bozulur ve "iki partisyon aynı satıra yazmaz" varsayımı çöker. Yeniden bölümleme araçları yok.
- Sahiplik için advisory lock kullanılıyor; işlem havuzlama modundaki PgBouncer ile **uyumsuz** (oturum kilidi).
- Quartz şeması, Quartz'ın resmî PostgreSQL betiğinden (sürüm 4.4.0 ile aynı depo) alınıp gömüldü; Quartz sürümü yükseltilirse betik yeniden eşitlenmeli.
  Kümeleme iki **süreç içi** düğümle (aynı veritabanı) test edildi, ayrı makinelerde değil.
- Uyarılar yalnızca tabloya yazılır; bildirim kanalı (e-posta, webhook) yok.
- API'de kimlik doğrulama yok. Sonsuz akış modu yok: `EventsPerPartition` sonlu.
- Verim ölçümü yukarıdaki kısıtlarla tek makinede yapıldı; çok düğümlü ölçekleme verimi ölçülmedi.
