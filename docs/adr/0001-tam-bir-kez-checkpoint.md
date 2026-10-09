# ADR 0001 — Tam bir kez etki: checkpoint ile özetler aynı işlemde

**Durum:** Kabul edildi

## Bağlam

Özetleri toplayarak biriktiriyoruz (`cnt = cnt + x`). Aynı parti iki kez uygulanırsa sayılar sessizce bozulur; hiç uygulanmazsa olaylar kaybolur.
İşlem ortada ölebilir, iki düğüm aynı partisyona yönelebilir.

## Karar

"Mesajı bir kez teslim etmek" mümkün değil; **etkiyi bir kez uygulamak** mümkün. Kaynak tekrar oynatılabilir (partisyon + offset), bu yüzden:

1. Parti, `partition_checkpoints.next_offset` değerinden okunur.
2. Tek veritabanı işlemi: `UPDATE partition_checkpoints SET next_offset = yeni WHERE partition = p AND next_offset = beklenen`.
   Etkilenen satır 1 değilse `FencingException` (başka bir yazıcı ilerletmiş), hiçbir şey yazılmaz.
3. Aynı işlemde özetler upsert edilir. Commit.

Çökme noktaları ve sonuçları: commit öncesi (işlem geri sarılır, parti baştan okunur), commit sonrası ama onaydan önce (checkpoint ilerlemiş, parti tekrar uygulanmaz).
Test, üç noktada %15'er olasılıkla rastgele çöküp özetleri bağımsız hesapla karşılaştırır.

Sahiplik: partisyon başına PostgreSQL advisory lock (verimlilik için; iki düğüm boşuna aynı işi yapmasın). **Doğruluk kilide bağlı değil**, CAS'a bağlı:
kilidi kaybetmiş bir zombie yazıcı da checkpoint karşılaştırmasında elenir.

## Sonuçlar

- Her parti bir tam gidiş-dönüş maliyeti öder (ölçüm: ~165 ms/50 bin olaylık parti); bu yüzden partiler büyük ve yazma ön özetlenmiş.
- Kaynak tekrar oynatılabilir olmak zorunda. Aksi halde bu desen uygulanamaz (bkz. README sınırlar).
