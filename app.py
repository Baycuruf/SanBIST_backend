import os
import time
import threading
import traceback
from datetime import datetime, time as dt_time, timedelta
import pytz
from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd

# --- Hisse Senedi Sembolleri ---
try:
    from bist100_symbols import BIST100_SYMBOLS
except ImportError:
    print("HATA: bist100_symbols.py dosyası bulunamadı.")
    BIST100_SYMBOLS = ["GARAN.IS", "AKBNK.IS", "THYAO.IS"]
    print(f"UYARI: Fallback sembol listesi kullanılıyor: {BIST100_SYMBOLS}")

# --- Döviz/Emtia Sembolleri (Yapı Düzeltildi) ---
COMMODITY_FOREX_SYMBOLS = [
    {'symbol': 'USDTRY=X', 'type': 'doviz', 'name': 'Dolar/TL'},
    {'symbol': 'EURTRY=X', 'type': 'doviz', 'name': 'Euro/TL'},
    {'symbol': 'GBPTRY=X', 'type': 'doviz', 'name': 'Sterlin/TL'},
    {'symbol': 'GC=F', 'type': 'maden_ons', 'name': 'Ons Altın (USD)'},
    {'symbol': 'SI=F', 'type': 'maden_ons', 'name': 'Ons Gümüş (USD)'},
    {'symbol': 'PL=F', 'type': 'maden_ons', 'name': 'Ons Platin (USD)'},
    {'symbol': 'EURUSD=X', 'type': 'doviz_capraz', 'name': 'Euro/Dolar Paritesi'},
]
ONS_TO_GRAM_DIVISOR = 31.1035

# --- Flask ve Zaman Dilimi Ayarları ---
app = Flask(__name__)
CORS(app)
istanbul_tz = pytz.timezone('Europe/Istanbul')

# --- Önbellekleme Ayarları ---
CACHE_DURATION_SECONDS = 15 * 60  # 15 dakika
cached_data = None
last_successful_fetch_time = 0
fetch_lock = threading.Lock()
fetch_in_progress_event = threading.Event()
fetch_complete_event = threading.Event()
stop_event = threading.Event()
background_thread = None

# --- YENİ: STATİK VERİ ÖNBELLEĞİ ---
# Bu sözlük, hisse adları ve sektörler gibi değişmeyen verileri tutar.
# Sadece uygulama başladığında 1 kez doldurulur.
STATIC_INFO_CACHE = {}

# --- Borsa Saatleri Kontrolü (Değişiklik yok) ---
def is_market_open(now_istanbul):
    day_of_week = now_istanbul.weekday() # Pazartesi=0, Pazar=6
    current_time = now_istanbul.time()
    if day_of_week >= 5: return False
    market_open_time = dt_time(10, 0)
    market_close_time = dt_time(18, 10) # yfinance gecikmesi için tolerans
    return market_open_time <= current_time <= market_close_time

# --- YENİ: STATİK VERİ ÇEKME FONKSİYONU ---
def fetch_static_company_info():
    """
    Uygulama başlarken SADECE BİR KEZ çalışır.
    Hisselerin adları ve sektörleri gibi değişmeyen verileri çeker.
    """
    global STATIC_INFO_CACHE
    if STATIC_INFO_CACHE: # Zaten doluysa tekrar çalıştırma
        return

    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Statik hisse bilgileri (Ad, Sektör) çekiliyor...")
    
    if not BIST100_SYMBOLS:
        print("BIST100 sembol listesi boş, statik veri çekilemiyor.")
        return

    symbols_str = " ".join(BIST100_SYMBOLS)
    tickers_bist = yf.Tickers(symbols_str)
    temp_cache = {}
    failed_infos = []

    for symbol in BIST100_SYMBOLS:
        try:
            full_info = tickers_bist.tickers[symbol].info
            long_name = full_info.get("longName")
            short_name = full_info.get("shortName")
            sector = full_info.get("sector", "Diğer")
            
            temp_cache[symbol] = {
                "name": long_name if long_name else (short_name if short_name else symbol),
                "sector": sector.replace(' ', '-').lower() if sector else 'bilinmiyor'
            }
        except Exception as e:
            failed_infos.append(symbol)
            temp_cache[symbol] = {
                "name": symbol,
                "sector": "bilinmiyor",
                "static_error": f"Statik info alınamadı: {e}"
            }
    
    STATIC_INFO_CACHE = temp_cache
    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Statik bilgiler çekildi. {len(failed_infos)} hata.")


# --- OPTİMİZE EDİLMİŞ Ana Veri Çekme Fonksiyonu ---
def fetch_and_cache_data():
    global cached_data, last_successful_fetch_time

    if fetch_in_progress_event.is_set():
        return False # Başka bir fetch sürüyor

    fetch_in_progress_event.set()
    fetch_complete_event.clear()
    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Dinamik veri çekme işlemi başlatılıyor...")
    success = False
    new_data = []
    
    try:
        if not BIST100_SYMBOLS:
            print("Uyarı: BIST100 sembol listesi boş, sadece maden/döviz çekilecek.")

        # === 1. BIST100 HİSSELERİNİ ÇEKME (OPTİMİZE EDİLDİ) ===
        if BIST100_SYMBOLS:
            try:
                symbols_str = " ".join(BIST100_SYMBOLS)
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 ({len(BIST100_SYMBOLS)} sembol) dinamik verileri (Fiyat/OHLCV) tek seferde çekiliyor...")
                
                # Tüm fiyat, hacim ve önceki gün kapanış verilerini TEK BİR ÇAĞRIDA al
                # period="2d" -> son 2 günü alır. iloc[-1] = bugün, iloc[-2] = dün (previousClose)
                data = yf.download(
                    symbols_str, period="2d", interval="1d",
                    group_by='ticker', progress=False, timeout=60
                )
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Dinamik veriler çekildi.")

                if data.empty:
                    raise Exception("yf.download 'period=2d' boş veri döndürdü.")

                # Çekilen veriyi işle
                for symbol in BIST100_SYMBOLS:
                    # Statik veriyi (Ad, Sektör) HAFİZADAN (CACHE) al
                    static_info = STATIC_INFO_CACHE.get(symbol, {"name": symbol, "sector": "bilinmiyor"})
                    
                    stock_result = {
                        "symbol": symbol,
                        "type": "hisse",
                        "name": static_info.get("name"),
                        "sector": static_info.get("sector"),
                        "price": None,
                        "previousClose": None,
                        "open": None,
                        "high": None,
                        "low": None,
                        "volume": None,
                        "timestamp": datetime.now().isoformat()
                    }

                    try:
                        # 'data' DataFrame'inden verileri al
                        stock_data = data[symbol]
                        if not stock_data.empty and len(stock_data) > 0:
                            # Son gün (bugün) verileri
                            today = stock_data.iloc[-1]
                            stock_result["price"] = today.get("Close")
                            stock_result["open"] = today.get("Open")
                            stock_result["high"] = today.get("High")
                            stock_result["low"] = today.get("Low")
                            stock_result["volume"] = today.get("Volume")
                            stock_result["timestamp"] = today.name.isoformat()

                            # Önceki gün (dün) kapanışı
                            if len(stock_data) > 1:
                                yesterday = stock_data.iloc[-2]
                                stock_result["previousClose"] = yesterday.get("Close")
                            else:
                                # Sadece 1 gün veri varsa (örn: yeni halka arz)
                                stock_result["previousClose"] = today.get("Open") # Fallback
                        
                        else:
                             stock_result["error"] = "Dinamik fiyat verisi bulunamadı."

                    except KeyError:
                        stock_result["error"] = "Hisse için 'yf.download' verisi bulunamadı (KeyError)."
                    except Exception as e:
                        stock_result["error"] = f"Hisse işlenirken hata: {e}"

                    new_data.append(stock_result)
                
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 dinamik verileri işlendi.")
            
            except Exception as e:
                print(f"HATA: BIST100 dinamik verileri çekilirken: {e}")
                traceback.print_exc()

        # === 2. MADEN VE DÖVİZ KURLARINI ÇEKME (Değişiklik yok, zaten hızlıydı) ===
        commodity_symbols_list = [item['symbol'] for item in COMMODITY_FOREX_SYMBOLS]
        
        if commodity_symbols_list:
            try:
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Maden/Döviz ({len(commodity_symbols_list)} sembol) çekiliyor...")
                tickers_comm = yf.Tickers(" ".join(commodity_symbols_list))
                
                for item in COMMODITY_FOREX_SYMBOLS:
                    symbol = item['symbol']
                    item_result = {
                        "symbol": symbol,
                        "type": item['type'],
                        "sector": "doviz" if "doviz" in item['type'] else "maden"
                    }
                    try:
                        info = tickers_comm.tickers[symbol].fast_info 
                        item_result["name"] = info.get("shortName", item.get('name', symbol))
                        item_result["price"] = info.get("lastPrice", info.get("regularMarketPrice"))
                        item_result["previousClose"] = info.get("previousClose", info.get("regularMarketPreviousClose"))
                        item_result["open"] = info.get("open", info.get("regularMarketOpen"))
                        item_result["high"] = info.get("dayHigh", info.get("regularMarketDayHigh"))
                        item_result["low"] = info.get("dayLow", info.get("regularMarketDayLow"))
                        item_result["volume"] = info.get("volume", info.get("regularMarketVolume"))
                        item_result["timestamp"] = datetime.now().isoformat()
                        
                        if item_result["price"] is None:
                            item_result["error"] = "Fiyat bilgisi alınamadı."
                            
                        new_data.append(item_result)
                    
                    except Exception as e:
                        print(f"Hata ({symbol} info çekilirken): {e}")
                        item_result["name"] = item.get('name', symbol)
                        item_result["error"] = "Veri çekilemedi."
                        new_data.append(item_result)
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Maden/Döviz çekildi.")

            except Exception as e:
                print(f"HATA: Maden/Döviz verileri çekilirken: {e}")
                traceback.print_exc()

        # === 3. SENTETİK HESAPLAMALAR (Değişiklik yok) ===
        try:
            usd_try_item = next((item for item in new_data if item["symbol"] == "USDTRY=X" and item.get("price")), None)
            ons_gold_item = next((item for item in new_data if item["symbol"] == "GC=F" and item.get("price")), None)
            ons_silver_item = next((item for item in new_data if item["symbol"] == "SI=F" and item.get("price")), None)
            ons_platinum_item = next((item for item in new_data if item["symbol"] == "PL=F" and item.get("price")), None)
            
            if usd_try_item and ons_gold_item:
                gram_gold_price = (ons_gold_item["price"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["price"]
                gram_gold_prev_close = None
                if ons_gold_item.get("previousClose") and usd_try_item.get("previousClose"):
                   gram_gold_prev_close = (ons_gold_item["previousClose"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["previousClose"]
                new_data.append({
                    "symbol": "GRAMALTIN", "type": "maden_gram", "name": "Gram Altın (TL)",
                    "price": gram_gold_price, "previousClose": gram_gold_prev_close,
                    "timestamp": datetime.now().isoformat(), "sector": "maden"
                })
            
            if usd_try_item and ons_silver_item:
                gram_silver_price = (ons_silver_item["price"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["price"]
                gram_silver_prev_close = None
                if ons_silver_item.get("previousClose") and usd_try_item.get("previousClose"):
                   gram_silver_prev_close = (ons_silver_item["previousClose"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["previousClose"]
                new_data.append({
                    "symbol": "GRAMGUMUS", "type": "maden_gram", "name": "Gram Gümüş (TL)",
                    "price": gram_silver_price, "previousClose": gram_silver_prev_close,
                    "timestamp": datetime.now().isoformat(), "sector": "maden"
                })

            if usd_try_item and ons_platinum_item:
                gram_platinum_price = (ons_platinum_item["price"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["price"]
                gram_platinum_prev_close = None
                if ons_platinum_item.get("previousClose") and usd_try_item.get("previousClose"):
                   gram_platinum_prev_close = (ons_platinum_item["previousClose"] / ONS_TO_GRAM_DIVISOR) * usd_try_item["previousClose"]
                new_data.append({
                    "symbol": "GRAMPLATIN", "type": "maden_gram", "name": "Gram Platin (TL)",
                    "price": gram_platinum_price, "previousClose": gram_platinum_prev_close,
                    "timestamp": datetime.now().isoformat(), "sector": "maden"
                })
            print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Sentetik gram fiyatları hesaplandı.")
        except Exception as e:
            print(f"HATA: Sentetik gram fiyatları hesaplanırken: {e}")
            traceback.print_exc()

        # === 4. ÖNBELLEĞİ GÜNCELLEME ===
        with fetch_lock:
            cached_data = new_data
            last_successful_fetch_time = time.time()
        success = True
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Önbellek güncellendi. Toplam {len(new_data)} varlık.")

    except Exception as e:
        print(f"Hata: Ana veri çekme işlemi başarısız: {e}")
        print(traceback.format_exc())
        success = False
    finally:
        fetch_in_progress_event.clear()
        fetch_complete_event.set()
        return success

# --- GÜNCELLENMİŞ Arka Plan Yenileme Thread Fonksiyonu ---
def background_refresher():
    global last_successful_fetch_time
    
    # 1. Adım: Önce statik verileri ÇEK (sadece 1 kez)
    print("Arka plan yenileyici başlatıldı: Önce statik veriler çekiliyor...")
    fetch_static_company_info() # Bu, yavaş olan ama 1 kez çalışan fonksiyon
    print("Statik veriler çekildi.")
    
    # 2. Adım: İlk dinamik veriyi ÇEK
    print("İlk dinamik veri çekme işlemi başlatılıyor...")
    fetch_and_cache_data()
    print("İlk dinamik veri çekme işlemi tamamlandı.")

    # 3. Adım: Döngüye gir (15 dakikada bir dinamik verileri çek)
    while not stop_event.is_set():
        try:
            now_istanbul = datetime.now(istanbul_tz)
            market_open = is_market_open(now_istanbul)
            cache_age = time.time() - last_successful_fetch_time
            
            # Piyasa açıksa ve önbellek 15 dakikadan eskiyse YENİLE
            if market_open and cache_age > CACHE_DURATION_SECONDS:
                if not fetch_in_progress_event.is_set():
                     print(f"[{now_istanbul.strftime('%H:%M:%S')}] [BG] Zaman aşımı, dinamik veri çekme başlatılıyor...")
                     fetch_and_cache_data()
            
            # Piyasa kapalıysa ve önbellek 1 saatten eskiyse YENİLE (kapanış verilerini almak için)
            elif not market_open and cache_age > 3600: 
                 if not fetch_in_progress_event.is_set():
                     print(f"[{now_istanbul.strftime('%H:%M:%S')}] [BG] Piyasa kapalı, kapanış verileri için yenileme...")
                     fetch_and_cache_data()
                     
            stop_event.wait(60) # 1 dakika bekle
        except Exception as e:
            print(f"Arka plan yenileyici hatası: {e}")
            print(traceback.format_exc())
            stop_event.wait(300) # Hata durumunda 5dk bekle


# --- Flask Endpoint: BIST100 (Değişiklik yok) ---
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


# --- Flask Endpoint: BIST100/COMPANIES (Değişiklik yok, mantık hala geçerli) ---
# Bu endpoint artık HİÇBİR ZAMAN veri çekme işlemini tetiklemeyecek,
# sadece arka plan thread'inin işini bitirmesini bekleyecek (eğer ilk istekse).
@app.route('/api/bist100/companies')
def get_bist100_companies():
    global cached_data, last_successful_fetch_time

    now_istanbul = datetime.now(istanbul_tz)

    # 1. Önbellek var mı? Varsa doğrudan sun.
    if cached_data is not None:
        return jsonify(cached_data)

    # 2. Önbellek yok (muhtemelen ilk istek). Fetch işlemi sürüyor mu?
    print(f"[{now_istanbul.strftime('%H:%M:%S')}] Önbellek boş. İlk veri çekme işlemi kontrol ediliyor...")

    if fetch_in_progress_event.is_set():
        print(f"[{now_istanbul.strftime('%H:%M:%S')}] İlk fetch işlemi sürüyor. Tamamlanması bekleniyor (max 60s)...")
        completed = fetch_complete_event.wait(timeout=60)
        
        if completed and cached_data:
            print(f"[{now_istanbul.strftime('%H:%M:%S')}] İlk fetch tamamlandı, veri sunuluyor.")
            return jsonify(cached_data)
        elif completed and not cached_data:
            return jsonify({"error": "Veri çekme işlemi tamamlandı ancak önbellek boş."}), 500
        else: # Timeout
            print(f"[{now_istanbul.strftime('%H:%M:%S')}] Fetch bekleme zaman aşımına uğradı.")
            return jsonify({"error": "Veri çekme işlemi zaman aşımına uğradı."}), 504
    else:
        # Bu durumun normalde olmaması gerekir (thread'in başlamaması)
        print(f"[{now_istanbul.strftime('%H:%M:%S')}] HATA: Önbellek boş ve fetch işlemi sürmüyor.")
        return jsonify({"error": "Sunucu başlatılıyor, lütfen birkaç saniye sonra tekrar deneyin."}), 503

# --- Uygulama Başlangıcı ---
# Gunicorn kullanırken 'if __name__ == "__main__":' bloğu ÇALIŞMAZ.
# Kodun doğrudan bu seviyede (global scope) olması gerekir.

print("Flask uygulaması başlatılıyor...")

if not os.environ.get("WERKZEUG_RUN_MAIN"):
     print("Arka plan thread başlatılıyor...")
     # NOT: Statik veri çekme artık 'background_refresher' İÇİNDE
     background_thread = threading.Thread(target=background_refresher, daemon=True)
     background_thread.start()

# 'app.run()' Gunicorn tarafından yönetildiği için burada olmamalı.
# Gunicorn 'app:app' nesnesini bulup kendisi çalıştırır.
print(f"Flask uygulaması Gunicorn tarafından yönetilmeye hazır. (PID: {os.getpid()})")
