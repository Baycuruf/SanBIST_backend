import os
from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import time
import threading
from datetime import datetime, time as dt_time, timedelta
import pytz # Zaman dilimi için 'pip install pytz'
import traceback # Hata ayıklama için

# Hisse sembol listesini içeren dosyayı import et
try:
    from bist100_symbols import BIST100_SYMBOLS
except ImportError:
    print("HATA: bist100_symbols.py dosyası bulunamadı veya BIST100_SYMBOLS listesi tanımlı değil.")
    BIST100_SYMBOLS = ["GARAN.IS", "AKBNK.IS", "THYAO.IS"] # Örnek
    print(f"UYARI: Fallback sembol listesi kullanılıyor: {BIST100_SYMBOLS}")

# --- SEMBOLLER (List of Dictionaries) ---
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
# --- BİTTİ ---

app = Flask(__name__)
CORS(app)

# --- Önbellekleme Ayarları ---
CACHE_DURATION_SECONDS = 15 * 60
cached_data = None
last_successful_fetch_time = 0
fetch_lock = threading.Lock()
fetch_in_progress_event = threading.Event()
fetch_complete_event = threading.Event()
background_thread = None
stop_event = threading.Event()

istanbul_tz = pytz.timezone('Europe/Istanbul')

# --- Borsa Saatleri Kontrolü ---
def is_market_open(now_istanbul):
    day_of_week = now_istanbul.weekday()
    current_time = now_istanbul.time()
    if day_of_week >= 5: return False
    market_open_time = dt_time(10, 0)
    market_close_time = dt_time(18, 10)
    return market_open_time <= current_time <= market_close_time

# --- Ana Veri Çekme Fonksiyonu (GÜNCELLENDİ) ---
def fetch_and_cache_data():
    global cached_data, last_successful_fetch_time

    if fetch_in_progress_event.is_set():
        return False

    fetch_in_progress_event.set()
    fetch_complete_event.clear()
    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Veri çekme işlemi başlatılıyor...")
    success = False
    new_data = []
    
    try:
        # === 1. BIST100 HİSSELERİNİ ÇEKME ===
        if BIST100_SYMBOLS:
            try:
                symbols_str = " ".join(BIST100_SYMBOLS)
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 ({len(BIST100_SYMBOLS)} sembol) çekiliyor...")
                
                # 1a. OHLCV (Günlük)
                try:
                    ohlcv_data = yf.download(
                        symbols_str, period="1d", interval="1d",
                        group_by='ticker', progress=False, timeout=60
                    )
                    print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] OHLCV verisi çekildi.")
                except Exception as ohlcv_err:
                     print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] UYARI: OHLCV verisi çekilemedi: {ohlcv_err}")
                     ohlcv_data = pd.DataFrame() # Boş DataFrame

                # 1b. Info (Fiyat, İsim, Sektör)
                tickers_bist = yf.Tickers(symbols_str)
                failed_infos_bist = []
                info_fetch_start = time.time()
                
                for symbol in BIST100_SYMBOLS:
                    stock_result = {"symbol": symbol, "type": "hisse"}
                    
                    # --- BÖLÜM 1: INFO (İSİM, FİYAT, SEKTÖR) ---
                    try:
                        full_info = tickers_bist.tickers[symbol].info
                        p_close = full_info.get("regularMarketPreviousClose")
                        current_price = full_info.get("regularMarketPrice") # Güncel fiyat
                        long_name = full_info.get("longName")
                        short_name = full_info.get("shortName")
                        sector = full_info.get("sector", "Diğer")
                        
                        stock_result["name"] = long_name if long_name else (short_name if short_name else symbol)
                        stock_result["previousClose"] = p_close
                        stock_result["price"] = current_price
                        stock_result["sector"] = sector.replace(' ', '-').lower() if sector else 'bilinmiyor'
                        
                        # Info'dan fiyat gelmezse hata olarak işaretle
                        if current_price is None:
                            stock_result["error"] = "Info: Fiyat alınamadı."

                    except Exception as info_err:
                        # .info çekilemezse (rate limit, vb.)
                        failed_infos_bist.append(symbol)
                        stock_result["name"] = symbol
                        stock_result["sector"] = "bilinmiyor"
                        stock_result["error"] = "Info verisi alınamadı."
                        # print(f"[{symbol}] Info hatası: {info_err}") # Debug

                    # --- BÖLÜM 2: OHLCV (OPEN, HIGH, LOW, VOLUME) ---
                    stock_ohlcv = None
                    stock_result.update({"open": None, "high": None, "low": None, "volume": None, "timestamp": datetime.now().isoformat()})
                    
                    try:
                        if not ohlcv_data.empty:
                            # Sembolün OHLCV verisinde olup olmadığını KONTROL ET
                            if ('Close', symbol) in ohlcv_data.columns:
                                temp_df = ohlcv_data.xs(symbol, level=1, axis=1).dropna()
                                if not temp_df.empty: 
                                    stock_ohlcv = temp_df.iloc[-1] # Son (dünkü) bar
                            
                        if stock_ohlcv is not None and not stock_ohlcv.empty:
                            stock_result["open"] = stock_ohlcv.get("Open")
                            stock_result["high"] = stock_ohlcv.get("High")
                            stock_result["low"] = stock_ohlcv.get("Low")
                            stock_result["volume"] = stock_ohlcv.get("Volume")
                            stock_result["timestamp"] = stock_ohlcv.name.isoformat() if hasattr(stock_ohlcv, 'name') else datetime.now().isoformat()
                            
                            # Eğer info'dan fiyat alamadıysak, OHLCV'den (dünkü kapanış) almayı dene
                            if stock_result.get("price") is None:
                                 stock_result["price"] = stock_ohlcv.get("Close")
                                 if stock_result.get("price") is not None:
                                     stock_result["error"] = None # Fiyat bulundu, hatayı temizle
                                     stock_result["warning"] = "Fiyat dünkü kapanış verisidir."
                    
                    except KeyError as ke:
                        # Bu normal bir durum, OHLCV verisi bulunamadı demektir. Hata basma.
                        # print(f"[{symbol}] OHLCV verisi bulunamadı (KeyError).")
                        pass 
                    except Exception as e:
                        print(f"Hata ({symbol} OHLCV işlenirken): {e}") # Sadece beklenmedik hataları bas
                
                    # --- BÖLÜM 3: SON KONTROL ---
                    if stock_result.get("price") is None and not stock_result.get("error"):
                         stock_result["error"] = "Güncel fiyat verisi alınamadı (info ve ohlcv)."
                    
                    new_data.append(stock_result)
                
                info_fetch_duration = time.time() - info_fetch_start
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 .info çekme süresi: {info_fetch_duration:.2f}s.")
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] BIST100 çekildi. {len(failed_infos_bist)} info hatası.")
            
            except Exception as e:
                print(f"HATA: BIST100 verileri çekilirken: {e}")
                traceback.print_exc()

        # === 2. MADEN VE DÖVİZ KURLARI (Değişiklik yok) ===
        commodity_symbols_list = [item['symbol'] for item in COMMODITY_FOREX_SYMBOLS]
        if commodity_symbols_list:
            try:
                print(f"[{datetime.now(istanbul_tz).strftime('%H:%M:%S')}] Maden/Döviz ({len(commodity_symbols_list)} sembol) çekiliyor...")
                tickers_comm = yf.Tickers(" ".join(commodity_symbols_list))
                
                for item in COMMODITY_FOREX_SYMBOLS:
                    symbol = item['symbol']
                    item_result = { "symbol": symbol, "type": item['type'], "sector": "doviz" if "doviz" in item['type'] else "maden" }
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
                        
                        if item_result["price"] is None: item_result["error"] = "Fiyat bilgisi alınamadı."
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

        # === 3. SENTETİK HESAPLAMALAR (Gram Platin dahil) ===
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
                    "open": None, "high": None, "low": None, "volume": None,
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
                    "open": None, "high": None, "low": None, "volume": None,
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
                    "open": None, "high": None, "low": None, "volume": None,
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

# --- Arka Plan Yenileme Thread Fonksiyonu (Değişiklik yok) ---
def background_refresher():
    global last_successful_fetch_time
    print("Arka plan yenileyici başlatıldı.")
    fetch_and_cache_data() 
    while not stop_event.is_set():
        try:
            now_istanbul = datetime.now(istanbul_tz)
            if is_market_open(now_istanbul):
                if time.time() - last_successful_fetch_time > CACHE_DURATION_SECONDS:
                    if not fetch_in_progress_event.is_set():
                         print(f"[{now_istanbul.strftime('%H:%M:%S')}] [BG] Zaman aşımı, arka plan fetch başlatılıyor...")
                         fetch_and_cache_data()
            stop_event.wait(60) 
        except Exception as e:
            print(f"Arka plan yenileyici hatası: {e}")
            print(traceback.format_exc())
            stop_event.wait(300)

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

# --- Flask Endpoint: /api/bist100/companies (OverflowError Düzeltilmiş) ---
@app.route('/api/bist100/companies')
def get_bist100_companies():
    global cached_data, last_successful_fetch_time

    now = time.time()
    now_istanbul = datetime.now(istanbul_tz)
    market_open = is_market_open(now_istanbul)
    cache_age = now - last_successful_fetch_time if last_successful_fetch_time > 0 else float('inf')

    if cached_data is not None:
        if not market_open or (market_open and cache_age < CACHE_DURATION_SECONDS):
            return jsonify(cached_data)

    # === HATA DÜZELTMESİ (OverflowError) ===
    cache_age_str = f"{int(cache_age)}s" if cache_age != float('inf') else "henüz yok"
    print(f"[{now_istanbul.strftime('%H:%M:%S')}] Önbellek geçersiz veya {cache_age_str}. Durum kontrol ediliyor...")
    # === DÜZELTME SONU ===

    if fetch_in_progress_event.is_set():
        print(f"[{now_istanbul.strftime('%H:%M:%S')}] Fetch işlemi sürüyor. Tamamlanması bekleniyor (max 60s)...")
        completed = fetch_complete_event.wait(timeout=60)
        if completed:
            print(f"[{now_istanbul.strftime('%H:%M:%S')}] Bekleme sonrası önbellek durumu kontrol ediliyor.")
            if cached_data:
                return jsonify(cached_data)
            else:
                return jsonify({"error": "Veri çekme işlemi beklenirken başarısız oldu."}), 500
        else: # Timeout
            print(f"[{now_istanbul.strftime('%H:%M:%S')}] Fetch bekleme zaman aşımına uğradı.")
            if cached_data:
                 print(f"[{now_istanbul.strftime('%H:%M:%S')}] Zaman aşımı sonrası eski önbellek sunuluyor.")
                 return jsonify(cached_data)
            else:
                 return jsonify({"error": "Veri çekme işlemi zaman aşımına uğradı."}), 504
    else:
        print(f"[{now_istanbul.strftime('%H:%M:%S')}] Yeni fetch işlemi tetikleniyor...")
        success = fetch_and_cache_data()
        if success and cached_data:
            return jsonify(cached_data)
        else:
            if cached_data:
                 print(f"[{now_istanbul.strftime('%H:%M:%S')}] Senkron fetch başarısız, eski önbellek sunuluyor.")
                 return jsonify(cached_data)
            else:
                 return jsonify({"error": "Veri çekilemedi ve önbellek boş."}), 500

# --- Uygulama Başlangıcı ---
if __name__ == '__main__':
    try:
        import pytz
    except ImportError:
        print("HATA: 'pytz' kütüphanesi bulunamadı. Lütfen 'pip install pytz' ile kurun.")
        exit(1)

    print("Flask uygulaması başlatılıyor...")
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
         print("Arka plan thread başlatılıyor...")
         background_thread = threading.Thread(target=background_refresher, daemon=True)
         background_thread.start()
    
    print(f"Flask sunucusu http://127.0.0.1:5000 adresinde başlatılıyor... (PID: {os.getpid()})")
    app.run(debug=True, port=5000)
