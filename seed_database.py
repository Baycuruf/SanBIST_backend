# seed_database.py
import yfinance as yf
from db_models import db, Company, Price
import peewee as pw
import time

# --- Kaynak Dosyalardan Verileri Al ---
try:
    from bist100_symbols import BIST100_SYMBOLS
except ImportError:
    print("HATA: bist100_symbols.py bulunamadı.")
    BIST100_SYMBOLS = []

# app.py'deki statik listeyi buraya kopyalıyoruz
# (Normalde bu iyi bir pratik değil ama şimdilik en hızlısı)
COMMODITY_FOREX_SYMBOLS = [
    {'symbol': 'USDTRY=X', 'type': 'doviz', 'name': 'Dolar/TL', 'sector': 'doviz'},
    {'symbol': 'EURTRY=X', 'type': 'doviz', 'name': 'Euro/TL', 'sector': 'doviz'},
    {'symbol': 'GBPTRY=X', 'type': 'doviz', 'name': 'Sterlin/TL', 'sector': 'doviz'},
    {'symbol': 'GC=F', 'type': 'maden_ons', 'name': 'Ons Altın (USD)', 'sector': 'maden'},
    {'symbol': 'SI=F', 'type': 'maden_ons', 'name': 'Ons Gümüş (USD)', 'sector': 'maden'},
    {'symbol': 'PL=F', 'type': 'maden_ons', 'name': 'Ons Platin (USD)', 'sector': 'maden'},
    {'symbol': 'EURUSD=X', 'type': 'doviz_capraz', 'name': 'Euro/Dolar Paritesi', 'sector': 'doviz'},
]

# app.py'deki sentetik varlıklar
SYNTHETIC_SYMBOLS = [
    {'symbol': 'GRAMALTIN', 'type': 'maden_gram', 'name': 'Gram Altın (TL)', 'sector': 'maden'},
    {'symbol': 'GRAMGUMUS', 'type': 'maden_gram', 'name': 'Gram Gümüş (TL)', 'sector': 'maden'},
    {'symbol': 'GRAMPLATIN', 'type': 'maden_gram', 'name': 'Gram Platin (TL)', 'sector': 'maden'},
]

def seed_companies():
    """
    Veritabanını statik varlık bilgileriyle doldurur (tohumlar).
    Bu işlem yavaştır ve sadece bir kez çalıştırılmalıdır.
    """
    db.connect()
    print("Veritabanı bağlantısı kuruldu.")
    
    # === 1. BIST100 HİSSELERİNİ İŞLEME ===
    if BIST100_SYMBOLS:
        print(f"\n{len(BIST100_SYMBOLS)} adet BIST100 hissesi işleniyor...")
        print("Bu işlem yfinance .info çağrıları nedeniyle 2-3 dakika sürebilir...")
        
        symbols_str = " ".join(BIST100_SYMBOLS)
        tickers_bist = yf.Tickers(symbols_str)
        failed_infos = []

        for symbol in BIST100_SYMBOLS:
            try:
                # O YAVAŞ ÇAĞRI: .info
                full_info = tickers_bist.tickers[symbol].info
                
                name = full_info.get("longName", full_info.get("shortName", symbol))
                sector = full_info.get("sector", "Diğer").replace(' ', '-').lower()

                # Veritabanına kaydet (veya güncelle)
                company, created = Company.get_or_create(
                    symbol=symbol,
                    defaults={
                        'name': name,
                        'type': 'hisse',
                        'sector': sector
                    }
                )
                
                if created:
                    print(f"[+] EKLENDİ: {symbol} ({name})")
                else:
                    # Eğer zaten varsa güncelle
                    company.name = name
                    company.sector = sector
                    company.save()
                    print(f"[=] GÜNCELLENDİ: {symbol}")

                # yfinance'i yormamak için kısa bir bekleme
                time.sleep(0.1) 

            except Exception as e:
                print(f"[!] HATA: {symbol} işlenemedi. Sebep: {e}")
                failed_infos.append(symbol)
                # Hata alsa bile bir kayıt oluşturalım ki fiyat takibi yapılabilsin
                Company.get_or_create(
                    symbol=symbol,
                    defaults={
                        'name': symbol, # İsim olarak sembolü kullan
                        'type': 'hisse',
                        'sector': 'bilinmiyor'
                    }
                )

        print(f"BIST100 tamamlandı. {len(failed_infos)} sembol .info hatası aldı (ama DB'ye eklendi).")

    # === 2. MADEN, DÖVİZ VE SENTETİK VARLIKLARI İŞLEME ===
    print("\nMaden, Döviz ve Sentetik varlıklar işleniyor...")
    
    # app.py'den aldığımız listeleri birleştiriyoruz
    all_other_assets = COMMODITY_FOREX_SYMBOLS + SYNTHETIC_SYMBOLS

    for asset in all_other_assets:
        try:
            company, created = Company.get_or_create(
                symbol=asset['symbol'],
                defaults={
                    'name': asset['name'],
                    'type': asset['type'],
                    'sector': asset.get('sector', 'diger')
                }
            )
            if created:
                print(f"[+] EKLENDİ (Diğer): {asset['symbol']} ({asset['name']})")
            else:
                # Varsa güncelle
                company.name = asset['name']
                company.type = asset['type']
                company.sector = asset.get('sector', 'diger')
                company.save()
                print(f"[=] GÜNCELLENDİ (Diğer): {asset['symbol']}")

        except Exception as e:
            print(f"[!] HATA (Diğer): {asset['symbol']} işlenemedi. Sebep: {e}")

    print("\nTohumlama işlemi tamamlandı.")
    db.close()

if __name__ == '__main__':
    print("UYARI: Bu script 'sanbist.db' veritabanını statik verilerle dolduracaktır.")
    print("Bu işlem BIST100 listesi için 2-3 dakika sürebilir.")
    # input("Devam etmek için ENTER'a basın (veya CTRL+C ile iptal edin)...")
    
    # Otomatik çalıştırmak için input'u yorum satırı yaptım.
    # Eğer bekletmek isterseniz üstteki satırı açıp alttakileri yorumlayın.
    
    start_time = time.time()
    seed_companies()
    end_time = time.time()
    print(f"\nToplam süre: {end_time - start_time:.2f} saniye.")
