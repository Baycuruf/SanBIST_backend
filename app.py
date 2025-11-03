# app.py (Yeni Veritabanı Odaklı Sürüm)
import os
import threading
import time
from datetime import datetime, time as dt_time, timedelta
import pytz
import traceback

from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import peewee as pw

# --- VERİTABANI MODELLERİ ---
# db_models.py dosyamızdan modelleri import ediyoruz
try:
    from db_models import db, Company, Price, create_tables
except ImportError:
    print("HATA: db_models.py bulunamadı.")
    exit(1)

# --- SEMBOL LİSTELERİ (Sadece Arka Plan İçin) ---
# Bu listeler sadece arka plan thread'inin neleri güncelleyeceğini bilmesi için gerekli.
try:
    from bist100_symbols import BIST100_SYMBOLS
except ImportError:
    print("UYARI: bist100_symbols.py bulunamadı. Hisse senetleri güncellenmeyecek.")
    BIST100_SYMBOLS = []

# Döviz/Maden sembollerini seed_database.py'den biliyoruz
COMMODITY_FOREX_SYMBOLS_LIST = [
    'USDTRY=X', 'EURTRY=X', 'GBPTRY=X', 'GC=F', 'SI=F', 'PL=F', 'EURUSD=X'
]
SYNTHETIC_SYMBOLS_LIST = ['GRAMALTIN', 'GRAMGUMUS', 'GRAMPLATIN']

# --- SABİTLER ---
ONS_TO_GRAM_DIVISOR = 31.1035
UPDATE_FREQUENCY_SECONDS = 15 * 60  # 15 dakika
istanbul_tz = pytz.timezone('Europe/Istanbul')

app = Flask(__name__)
CORS(app)

# Arka plan thread'i için durdurma olayı
stop_event = threading.Event()

# --- VERİTABANI BAĞLANTI YÖNETİMİ ---
# Flask'ın her API isteğinden önce veritabanını açmasını
# ve her istekten sonra kapatmasını sağlıyoruz.
@app.before_request
def before_request():
    if db.is_closed():
        db.connect()

@app.after_request
def after_request(response):
    if not db.is_closed():
        db.close()
    return response

# --- Borsa Saatleri Kontrolü (Değişiklik yok) ---
def is_market_open(now_istanbul):
    day_of_week = now_istanbul.weekday() # Pazartesi=0, Pazar=6
    current_time = now_istanbul.time()
    if day_of_week >= 5: return False
    market_open_time = dt_time(10, 0)
    market_close_time = dt_time(18, 10)
    return market_open_time <= current_time <= market_close_time

# --- YENİ: ARKA PLAN FİYAT GÜNCELLEME GÖREVİ ---
# Bu fonksiyonun TEK GÖREVİ fiyattları çekip 'Price' tablosunu güncellemektir.
# Artık .info ile Sektör/İsim çekmez!
def update_prices_task():
    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Arka plan fiyat güncelleme başladı...")
    
    # Güncellenecek fiyat verilerini bu listede toplayacağız
    prices_data_list = []
    
    # === 1. BIST100 HİSSELERİNİ ÇEKME (HIZLI YÖNTEM) ===
    try:
        if BIST100_SYMBOLS:
            print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 ({len(BIST100_SYMBOLS)} sembol) çekiliyor...")
            # 2 günlük veri çekiyoruz:
            # iloc[-1] (bugün) -> price, open, high, low, volume
            # iloc[-2] (dün)   -> previousClose
            data = yf.download(
                " ".join(BIST100_SYMBOLS),
                period="2d", # 2 gün yeterli
                interval="1d", # Günlük veri
                progress=False,
                timeout=60
            )
            
            if not data.empty and 'Close' in data:
                # Veriyi daha kolay işlemek için sembol bazlı yeniden düzenle
                data = data.stack(level=1).rename_axis(['Date', 'Symbol']).reset_index()

                today_data = data[data['Date'] == data['Date'].max()]
                yesterday_data = data[data['Date'] == data['Date'].min()]

                for symbol in BIST100_SYMBOLS:
                    today = today_data[today_data['Symbol'] == symbol].iloc[0:1] # Tek satırlık DF
                    yesterday = yesterday_data[yesterday_data['Symbol'] == symbol].iloc[0:1] # Tek satırlık DF

                    if today.empty:
                        prices_data_list.append({'symbol': symbol, 'error': 'Bugün verisi yok', 'timestamp': datetime.now()})
                        continue

                    price_val = today['Close'].values[0] if not today.empty else None
                    prev_close_val = yesterday['Close'].values[0] if not yesterday.empty else None
                    
                    # Eğer bugün fiyat yoksa (örn. tahta kapalı) ama dün varsa, dünkü fiyatı kullan
                    if pd.isna(price_val) and not pd.isna(prev_close_val):
                         price_val = prev_close_val

                    if not pd.isna(price_val):
                        prices_data_list.append({
                            'symbol': symbol,
                            'price': price_val,
                            'previousClose': prev_close_val,
                            'open': today['Open'].values[0] if not today.empty else None,
                            'high': today['High'].values[0] if not today.empty else None,
                            'low': today['Low'].values[0] if not today.empty else None,
                            'volume': today['Volume'].values[0] if not today.empty else None,
                            'timestamp': datetime.now(),
                            'error': None
                        })
                    else:
                        prices_data_list.append({'symbol': symbol, 'error': 'yf.download verisi bulunamadı', 'timestamp': datetime.now()})
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 hisse fiyatları çekildi.")
    except Exception as e:
        print(f"HATA (BIST100 yf.download): {e}")
        traceback.print_exc()

    # === 2. DÖVİZ/MADEN ÇEKME (HIZLI YÖNTEM) ===
    try:
        if COMMODITY_FOREX_SYMBOLS_LIST:
            print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Döviz/Maden ({len(COMMODITY_FOREX_SYMBOLS_LIST)} sembol) çekiliyor...")
            tickers_comm = yf.Tickers(" ".join(COMMODITY_FOREX_SYMBOLS_LIST))
            
            for symbol in COMMODITY_FOREX_SYMBOLS_LIST:
                try:
                    info = tickers_comm.tickers[symbol].fast_info
                    price_val = info.get("lastPrice", info.get("regularMarketPrice"))
                    if price_val:
                        prices_data_list.append({
                            'symbol': symbol,
                            'price': price_val,
                            'previousClose': info.get("previousClose", info.get("regularMarketPreviousClose")),
                            'open': info.get("open", info.get("regularMarketOpen")),
                            'high': info.get("dayHigh", info.get("regularMarketDayHigh")),
                            'low': info.get("dayLow", info.get("regularMarketDayLow")),
                            'volume': info.get("volume", info.get("regularMarketVolume")),
                            'timestamp': datetime.now(),
                            'error': None
                        })
                    else:
                        prices_data_list.append({'symbol': symbol, 'error': 'fast_info fiyatı yok', 'timestamp': datetime.now()})
                except Exception as e:
                    print(f"HATA ({symbol} fast_info): {e}")
                    prices_data_list.append({'symbol': symbol, 'error': str(e), 'timestamp': datetime.now()})
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Döviz/Maden fiyatları çekildi.")
    except Exception as e:
        print(f"HATA (Döviz/Maden Tickers): {e}")
        traceback.print_exc()

    # === 3. SENTETİK VARLIKLARI HESAPLAMA ===
    try:
        # Az önce çektiğimiz güncel verilerden (DB'den değil) hesaplıyoruz
        usd_try_item = next((p for p in prices_data_list if p["symbol"] == "USDTRY=X" and p.get("price")), None)
        ons_gold_item = next((p for p in prices_data_list if p["symbol"] == "GC=F" and p.get("price")), None)
        ons_silver_item = next((p for p in prices_data_list if p["symbol"] == "SI=F" and p.get("price")), None)
        ons_platinum_item = next((p for p in prices_data_list if p["symbol"] == "PL=F" and p.get("price")), None)

        def calculate_synthetic(base_item, name, symbol):
            if usd_try_item and base_item:
                price = (base_item["price"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["price"]
                prev_close = None
                if base_item.get("previousClose") and usd_try_item.get("previousClose"):
                   prev_close = (base_item["previousClose"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["previousClose"]
                prices_data_list.append({
                    'symbol': symbol, 'price': price, 'previousClose': prev_close, 'timestamp': datetime.now()
                })
        
        calculate_synthetic(ons_gold_item, "Gram Altın (TL)", "GRAMALTIN")
        calculate_synthetic(ons_silver_item, "Gram Gümüş (TL)", "GRAMGUMUS")
        calculate_synthetic(ons_platinum_item, "Gram Platin (TL)", "GRAMPLATIN")
        
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Sentetik gram fiyatları hesaplandı.")
    except Exception as e:
        print(f"HATA (Sentetik Fiyatlar): {e}")
        traceback.print_exc()

    # === 4. VERİTABANINA TOPLU YAZMA ===
    if prices_data_list:
        try:
            # Thread güvenliği için bu thread'in kendi bağlantısını aç/kapat yapması en iyisi
            if db.is_closed(): db.connect()
            
            with db.atomic():
                # Peewee'nin sihirli komutu:
                # 'symbol' (primary key) eşleşirse GÜNCELLE, eşleşmezse YENİ EKLE.
                # 'on_conflict' SQLite için 'replace' anlamına gelir.
                Price.replace_many(prices_data_list).execute()
                
            print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Veritabanı (Price tablosu) {len(prices_data_list)} kayıtla güncellendi.")
        except Exception as e:
            print(f"HATA (Veritabanı Yazma): {e}")
            traceback.print_exc()
        finally:
            if not db.is_closed(): db.close()
    else:
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Güncellenecek fiyat verisi bulunamadı.")
        
    return True

# --- YENİ: BASİTLEŞTİRİLMİŞ ARKA PLAN THREAD'İ ---
def background_refresher():
    print("Arka plan fiyat güncelleyici başlatıldı.")
    
    # Sunucu başlarken ilk veriyi hemen çekelim
    # (Thread'in kendi bağlantısını yönetmesi önemli)
    try:
        update_prices_task()
    except Exception as e:
        print(f"İlk çalıştırmada hata: {e}")

    last_update_time = time.time()

    while not stop_event.is_set():
        try:
            now_istanbul = datetime.now(istanbul_tz)
            if is_market_open(now_istanbul):
                if time.time() - last_update_time > UPDATE_FREQUENCY_SECONDS:
                    print(f"[{now_istanbul.strftime('%H:%M:%S')}] [BG] Zamanı geldi, fiyat güncelleme tetikleniyor...")
                    update_prices_task()
                    last_update_time = time.time()
            
            stop_event.wait(60) # 1 dakika bekle
        except Exception as e:
            print(f"Arka plan yenileyici hatası: {e}")
            traceback.print_exc()
            stop_event.wait(300) # Hata durumunda 5dk bekle

# --- Flask Endpoint: BIST100 (Değişiklik yok, bu zaten hızlı) ---
@app.route('/api/bist100')
def get_bist100_index():
    try:
        ticker = yf.Ticker("XU100.IS")
        info = ticker.fast_info
        data = {
            "symbol": info.get("symbol", "XU100.IS"),
            "shortName": info.get("shortName", "BIST 100"),
            "regularMarketPrice": info.get("lastPrice"),
            "regularMarketOpen": info.get("open"),
            "regularMarketDayHigh": info.get("dayHigh"),
            "regularMarketDayLow": info.get("dayLow"),
            "regularMarketPreviousClose": info.get("previousClose"),
            "marketState": info.get("marketState", "UNKNOWN")
        }
        if data["marketState"] == "UNKNOWN" and data["regularMarketPrice"] is not None:
             now_ist = datetime.now(istanbul_tz)
             data["marketState"] = "REGULAR" if is_market_open(now_ist) else "CLOSED"
        return jsonify(data)
    except Exception as e:
        print(f"yfinance hatası (XU100.IS): {e}")
        return jsonify({"error": "Endeks verisi çekilemedi.", "symbol": "XU100.IS", "shortName": "BIST 100", "price": None}), 500

# --- YENİ: IŞIK HIZINDA API ENDPOINT ---
# Artık cache, lock, event, bekleme, timeout HİÇBİRİ YOK!
@app.route('/api/bist100/companies')
def get_bist100_companies():
    try:
        # Tek yapmamız gereken veritabanından okumak.
        # Peewee'nin JOIN sorgusu: Company ve Price tablolarını birleştir.
        
        # Company tablosunu seç, Price tablosunu da "LEFT JOIN" ile bağla.
        # (LEFT_OUTER: Company'de olan ama Price'ta henüz olmayanları da getirir)
        query = (Company
                 .select(Company, Price) # İki tablodan da verileri al
                 .join(Price, pw.JOIN.LEFT_OUTER, on=(Company.symbol == Price.symbol))
                )

        results_list = []
        for company in query:
            # Temel (Statik) veriler
            data = {
                "symbol": company.symbol,
                "name": company.name,
                "type": company.type,
                "sector": company.sector,
            }
            
            # Fiyat (Dinamik) verileri
            # (company.price.symbol_id kontrolü, Price verisinin olup olmadığını kontrol eder)
            if hasattr(company, 'price') and company.price.symbol_id is not None:
                data.update({
                    "price": company.price.price,
                    "previousClose": company.price.previousClose,
                    "open": company.price.open,
                    "high": company.price.high,
                    "low": company.price.low,
                    "volume": company.price.volume,
                    "timestamp": company.price.timestamp.isoformat() if company.price.timestamp else None,
                    "error": company.price.error
                })
            else:
                # Price tablosunda henüz verisi yoksa (örn. seed'den sonra ilk fetch bekleniyorsa)
                 data.update({
                    "price": None,
                    "previousClose": None,
                    "open": None, "high": None, "low": None, "volume": None,
                    "timestamp": None,
                    "error": "Henüz fiyat verisi alınmadı."
                })
            
            results_list.append(data)

        # Hızlıca JSON döndür
        return jsonify(results_list)

    except Exception as e:
        print(f"HATA (/api/bist100/companies): {e}")
        traceback.print_exc()
        return jsonify({"error": "Veritabanı sorgusunda hata oluştu.", "details": str(e)}), 500

# --- Uygulama Başlangıcı ---
if __name__ == '__main__':
    # Veritabanı ve tablolar var mı diye son bir kontrol
    if not db.table_exists('company') or not db.table_exists('price'):
        print("HATA: 'company' veya 'price' tablosu bulunamadı.")
        print("Lütfen önce 'python db_models.py' komutunu çalıştırın.")
        print("Eğer çalıştırdıysanız, 'python seed_database.py' komutunu çalıştırın.")
        exit(1)
        
    print("Flask uygulaması başlatılıyor...")
    
    # Arka plan thread'ini başlat (Flask debug modu çift çalıştırmasın diye kontrol)
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
         print("Arka plan fiyat güncelleyici thread başlatılıyor...")
         background_thread = threading.Thread(target=background_refresher, daemon=True)
         background_thread.start()
    
    # Port 5000
    print(f"Flask sunucusu http://127.0.0.1:5000 adresinde başlatılıyor...")
    app.run(debug=True, port=5000)
