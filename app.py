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
import numpy as np

# --- Hisse Senedi Sembolleri ---
try:
    from bist100_symbols import BIST100_SYMBOLS
except ImportError:
    print("HATA: bist100_symbols.py dosyası bulunamadı.")
    BIST100_SYMBOLS = ["GARAN.IS", "AKBNK.IS", "THYAO.IS"] # Fallback
    print(f"UYARI: Fallback sembol listesi kullanılıyor: {BIST100_SYMBOLS}")

# --- Döviz/Emtia Sembolleri ---
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

# --- Önbellekleme Ayarları (GLOBAL DEĞİŞKENLER) ---
# --workers 1 sayesinde bu değişkenler tüm thread'ler arasında paylaşılacak
CACHE_DURATION_SECONDS = 15 * 60  # 15 dakika
cached_data = {} # Sözlük olarak başlat
last_successful_fetch_time = 0
fetch_lock = threading.Lock()
fetch_in_progress_event = threading.Event()
fetch_complete_event = threading.Event()
stop_event = threading.Event()
background_thread = None

# Aşamalı yükleme için (25'erli gruplar)
CHUNK_SIZE = 25 

# Statik Veri Önbelleği (Değişmeyen Ad/Sektör)
STATIC_INFO_CACHE = {}

# --- Borsa Saatleri Kontrolü ---
def is_market_open(now_istanbul):
    day_of_week = now_istanbul.weekday() # Pazartesi=0
    current_time = now_istanbul.time()
    if day_of_week >= 5: return False
    market_open_time = dt_time(10, 0)
    market_close_time = dt_time(18, 10)
    return market_open_time <= current_time <= market_close_time

# --- Statik Veri Çekme Fonksiyonu (Ad/Sektör) ---
def fetch_static_company_info(symbols_list):
    global STATIC_INFO_CACHE
    if not symbols_list: return

    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Statik bilgiler (Ad/Sektör) {len(symbols_list)} sembol için çekiliyor...")
    symbols_str = " ".join(symbols_list)
    tickers_bist = yf.Tickers(symbols_str)
    failed_infos = 0

    for symbol in symbols_list:
        try:
            full_info = tickers_bist.tickers[symbol].info
            long_name = full_info.get("LongName", full_info.get("longName"))
            short_name = full_info.get("shortName")
            sector = full_info.get("sector", "Diğer")
            STATIC_INFO_CACHE[symbol] = {
                "name": long_name if long_name else (short_name if short_name else symbol),
                "sector": sector.replace(' ', '-').lower() if sector else 'bilinmiyor'
            }
        except Exception:
            failed_infos += 1
            STATIC_INFO_CACHE[symbol] = {"name": symbol, "sector": "bilinmiyor"}
    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Statik bilgiler çekildi. {failed_infos} hata.")

# --- Dinamik Veri Çekme Fonksiyonu (Fiyat/OHLCV) ---
def fetch_dynamic_price_data(symbols_list):
    if not symbols_list: return []

    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Dinamik veriler (Fiyat/OHLCV) {len(symbols_list)} sembol için çekiliyor...")
    results_list = []
    try:
        data = yf.download(
            " ".join(symbols_list), period="2d", interval="1d",
            group_by='ticker', progress=False, timeout=60
        )
        if data.empty: raise Exception("yf.download 'period=2d' boş veri döndürdü.")

        for symbol in symbols_list:
            static_info = STATIC_INFO_CACHE.get(symbol, {"name": symbol, "sector": "bilinmiyor"})
            stock_result = {"symbol": symbol, "type": "hisse", **static_info}
            
            try:
                stock_data = data[symbol] if len(symbols_list) > 1 else data
                if not stock_data.empty and len(stock_data) > 0:
                    today = stock_data.iloc[-1]
                    stock_result.update({
                        "price": today.get("Close"), "open": today.get("Open"),
                        "high": today.get("High"), "low": today.get("Low"),
                        "volume": today.get("Volume"), "timestamp": today.name.isoformat()
                    })
                    if len(stock_data) > 1:
                        stock_result["previousClose"] = stock_data.iloc[-2].get("Close")
                    else:
                        stock_result["previousClose"] = today.get("Open")
                else:
                     stock_result["error"] = "Dinamik fiyat verisi bulunamadı."
            except KeyError:
                stock_result["error"] = "Hisse için 'yf.download' verisi bulunamadı."
            results_list.append(stock_result)
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Dinamik veriler işlendi.")
    except Exception as e:
        print(f"HATA: BIST100 dinamik verileri çekilirken: {e}")
        for symbol in symbols_list:
             static_info = STATIC_INFO_CACHE.get(symbol, {"name": symbol, "sector": "bilinmiyor"})
             results_list.append({"symbol": symbol, "type": "hisse", **static_info, "error": f"Dinamik veri çekme hatası: {e}"})
    return results_list

# --- Döviz/Emtia Çekme Fonksiyonu (Güvenilir) ---
def fetch_commodities_data():
    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Maden/Döviz (yf.download ile) çekiliyor...")
    new_data = []
    symbol_map = {item['symbol']: item for item in COMMODITY_FOREX_SYMBOLS}
    symbols_list = list(symbol_map.keys())
    if not symbols_list: return []
        
    try:
        data = yf.download(
            " ".join(symbols_list), period="2d", interval="1d",
            group_by='ticker', progress=False, timeout=60
        )
        if data.empty: raise Exception("yf.download (commodities) boş veri döndürdü.")

        for symbol in symbols_list:
            item_template = symbol_map[symbol]
            item_result = {
                "symbol": symbol, "type": item_template['type'], "name": item_template['name'],
                "sector": "doviz" if "doviz" in item_template['type'] else "maden"
            }
            try:
                stock_data = data[symbol] if len(symbols_list) > 1 else data
                if not stock_data.empty and len(stock_data) > 0:
                    today = stock_data.iloc[-1]
                    item_result.update({
                        "price": today.get("Close", today.get("Adj Close")),
                        "open": today.get("Open"), "high": today.get("High"),
                        "low": today.get("Low"), "volume": today.get("Volume"),
                        "timestamp": today.name.isoformat()
                    })
                    if len(stock_data) > 1:
                        item_result["previousClose"] = stock_data.iloc[-2].get("Close", stock_data.iloc[-2].get("Adj Close"))
                    else:
                        item_result["previousClose"] = today.get("Open")
                else:
                    item_result["error"] = "Dinamik fiyat verisi bulunamadı."
            except KeyError:
                item_result["error"] = "Emtia için 'yf.download' verisi bulunamadı."
            new_data.append(item_result)
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Maden/Döviz çekildi (yf.download ile).")
    except Exception as e:
        print(f"HATA: Maden/Döviz verileri çekilirken: {e}")
        for symbol in symbols_list:
             item_template = symbol_map[symbol]
             new_data.append({"symbol": symbol, **item_template, "error": f"Dinamik veri çekme hatası: {e}"})

    # Sentetik Hesaplamalar
    try:
        usd_try_item = next((item for item in new_data if item["symbol"] == "USDTRY=X" and item.get("price")), None)
        ons_gold_item = next((item for item in new_data if item["symbol"] == "GC=F" and item.get("price")), None)
        ons_silver_item = next((item for item in new_data if item["symbol"] == "SI=F" and item.get("price")), None)
        ons_platinum_item = next((item for item in new_data if item["symbol"] == "PL=F" and item.get("price")), None)
        
        def calculate_synthetic(ons_item, usd_item, symbol, name):
            if usd_item and ons_item and ons_item.get("price") is not None and usd_item.get("price") is not None:
                price = (ons_item["price"] / ONS_TO_GRAM_DIVISOR) * usd_item["price"]
                prev_close = None
                if ons_item.get("previousClose") and usd_item.get("previousClose"):
                   prev_close = (ons_item["previousClose"] / ONS_TO_GRAM_DIVISOR) * usd_item["previousClose"]
                return {"symbol": symbol, "type": "maden_gram", "name": name, "price": price, "previousClose": prev_close, "timestamp": datetime.now().isoformat(), "sector": "maden"}
            return None

        for item in [
            calculate_synthetic(ons_gold_item, usd_try_item, "GRAMALTIN", "Gram Altın (TL)"),
            calculate_synthetic(ons_silver_item, usd_try_item, "GRAMGUMUS", "Gram Gümüş (TL)"),
            calculate_synthetic(ons_platinum_item, usd_try_item, "GRAMPLATIN", "Gram Platin (TL)")
        ]:
            if item: new_data.append(item)
        print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Sentetik gram fiyatları hesaplandı.")
    except Exception as e:
        print(f"HATA: Sentetik gram fiyatları hesaplanırken: {e}")
    return new_data

# --- Arka Plan Yenileme Fonksiyonu (DÜZELTİLMİŞ) ---
def background_refresher():
    global cached_data, last_successful_fetch_time, fetch_in_progress_event, fetch_complete_event

    print("Arka plan yenileyici başlatıldı.")
    fetch_in_progress_event.set() # Başlangıçta fetch sürüyor
    
    try:
        # --- 1. AŞAMA: Başlangıç Yüklemesi (Aşamalı) ---
        print("Başlangıç yüklemesi (Aşamalı) başlıyor...")
        
        # 1. Önce Döviz/Emtia çek (Hızlı ve Güvenilir)
        commodity_data = fetch_commodities_data()
        with fetch_lock:
            for item in commodity_data:
                cached_data[item['symbol']] = item
        print(f"İlk Döviz/Emtia verisi yüklendi ({len(commodity_data)} varlık).")
        
        # 2. BIST100 listesini chunk'lara böl
        # DÜZELTME: 100 // 25 = 4 chunk. Bu, 4 aşama (her biri 25 sembol) oluşturur.
        num_chunks = max(1, len(BIST100_SYMBOLS) // CHUNK_SIZE) 
        symbol_chunks = np.array_split(BIST100_SYMBOLS, num_chunks)
        
        # 3. BIST100 chunk'larını SIRA İLE işle
        for i, chunk in enumerate(symbol_chunks):
            chunk_list = chunk.tolist()
            print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 AŞAMA {i+1}/{len(symbol_chunks)} ({len(chunk_list)} sembol) başlıyor...")
            
            fetch_static_company_info(chunk_list)
            dynamic_data = fetch_dynamic_price_data(chunk_list)
            
            with fetch_lock:
                for item in dynamic_data:
                    cached_data[item['symbol']] = item
            
            print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] AŞAMA {i+1} tamamlandı. Önbellekte {len(cached_data)} varlık var.")
            
            if i == 0:
                # İLK AŞAMA (Döviz + 25 hisse) BİTTİĞİNDE API'yi aç
                last_successful_fetch_time = time.time()
                fetch_in_progress_event.clear() # Fetch bitti
                fetch_complete_event.set() # API'ye "veri hazır" sinyali gönder
                print(f"İLK AŞAMA tamamlandı. API 'veri hazır' sinyali aldı.")
            
            time.sleep(2) # yfinance rate limit için bekle

        print("Tüm aşamalı yükleme tamamlandı.")
        last_successful_fetch_time = time.time()
        
    except Exception as e:
        print(f"HATA: Başlangıç yüklemesi başarısız: {e}")
        traceback.print_exc()
        fetch_in_progress_event.clear() # Hata durumunda da API'yi aç
        fetch_complete_event.set()

    # --- 2. AŞAMA: Normal Yenileme Döngüsü (15 Dk'da bir) ---
    print("Normal yenileme döngüsü (15dk) başlıyor...")
    while not stop_event.is_set():
        try:
            now_istanbul = datetime.now(istanbul_tz)
            wait_time = 60 # Varsayılan kontrol 60sn
            
            if is_market_open(now_istanbul):
                if time.time() - last_successful_fetch_time > CACHE_DURATION_SECONDS:
                    print(f"[{now_istanbul.strftime('%H:%M:%S')}] [BG] Zaman aşımı, tam yenileme başlıyor...")
                    fetch_in_progress_event.set()
                    
                    commodity_data = fetch_commodities_data()
                    # Statik info'yu tekrar çekmeye gerek yok, cache'de var
                    dynamic_data = fetch_dynamic_price_data(BIST100_SYMBOLS)
                    
                    with fetch_lock:
                        for item in (dynamic_data + commodity_data):
                            cached_data[item['symbol']] = item
                    
                    last_successful_fetch_time = time.time()
                    fetch_in_progress_event.clear()
                    print(f"[{now_istanbul.strftime('%H:%M:%S')}] [BG] Tam yenileme tamamlandı. {len(cached_data)} varlık.")
            
            stop_event.wait(wait_time) # 60 saniye bekle
            
        except Exception as e:
            print(f"Arka plan yenileyici hatası: {e}")
            traceback.print_exc()
            stop_event.wait(300) # Hata durumunda 5dk bekle

# --- Flask Endpoint: BIST100 (Endeks) ---
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
        return jsonify(data)
    except Exception as e:
        print(f"yfinance hatası (XU100.IS): {e}")
        return jsonify({"error": "Endeks verisi çekilemedi.", "price": None}), 500

# --- Flask Endpoint: BIST100/COMPANIES (DÜZELTİLMİŞ) ---
@app.route('/api/bist100/companies')
def get_bist100_companies():
    global cached_data, fetch_in_progress_event, fetch_complete_event

    # 1. Önbellek dolu mu? (Artık {} değil, len() > 0 mı?)
    # --workers 1 sayesinde buradaki 'cached_data' her zaman günceldir.
    if len(cached_data) > 0:
        return jsonify(list(cached_data.values()))

    # 2. Önbellek boş. Başlangıç yüklemesi (ilk chunk) sürüyor mu?
    now_istanbul = datetime.now(istanbul_tz)
    print(f"[{now_istanbul.strftime('%H:%M:%S')}] Önbellek boş. İlk aşamanın tamamlanması bekleniyor...")

    if fetch_in_progress_event.is_set():
        # İlk chunk'ın bitmesini 60sn bekle
        completed = fetch_complete_event.wait(timeout=60) 
        
        if completed and len(cached_data) > 0:
            print(f"[{now_istanbul.strftime('%H:%M:%S')}] İlk aşama tamamlandı, veri sunuluyor.")
            return jsonify(list(cached_data.values()))
        elif completed:
            return jsonify({"error": "Veri çekme işlemi tamamlandı ancak önbellek boş."}), 500
        else: # Timeout
            print(f"[{now_istanbul.strftime('%H:%M:%S')}] Fetch bekleme zaman aşımına uğradı (60s).")
            return jsonify({"error": "Veri çekme işlemi zaman aşımına uğradı."}), 504
    else:
        # Thread çöktüyse bu olabilir
        print(f"[{now_istanbul.strftime('%H:%M:%S')}] HATA: Önbellek boş ve fetch işlemi sürmüyor.")
        return jsonify({"error": "Sunucu başlatılıyor, lütfen birkaç saniye sonra tekrar deneyin."}), 503

# --- Uygulama Başlangıcı ---
print("Flask uygulaması başlatılıyor...")

if not os.environ.get("WERKZEUG_RUN_MAIN"):
     print("Arka plan thread başlatılıyor (Aşamalı Yükleme)...")
     background_thread = threading.Thread(target=background_refresher, daemon=True)
     background_thread.start()

print(f"Flask uygulaması Gunicorn tarafından yönetilmeye hazır. (PID: {os.getpid()})")
