# db_models.py
import peewee as pw
from datetime import datetime

# Veritabanı dosyamızı tanımlıyoruz. 
# Proje dizininde 'sanbist.db' adında bir dosya oluşturulacak.
db = pw.SqliteDatabase('sanbist.db')

# Tüm modellerimiz için temel bir sınıf
class BaseModel(pw.Model):
    class Meta:
        database = db

# --- 1. STATİK VERİ TABLOSU ---
# Şirketlerin, dövizlerin ve madenlerin değişmeyen bilgileri
class Company(BaseModel):
    """
    Varlıkların (hisse, döviz, maden) statik bilgilerini tutar.
    Bu tablo 'seed_database.py' ile bir kez doldurulur ve nadiren güncellenir.
    """
    symbol = pw.CharField(unique=True, primary_key=True, max_length=20)
    name = pw.CharField(max_length=255)
    type = pw.CharField(max_length=50, index=True) # hisse, doviz, maden_ons, maden_gram
    sector = pw.CharField(max_length=100, null=True)

# --- 2. DİNAMİK VERİ TABLOSU ---
# Fiyatların sürekli güncellenen anlık verileri
class Price(BaseModel):
    """
    Varlıkların anlık fiyat bilgilerini tutar.
    Bu tablo arka plan thread'i tarafından sürekli güncellenir.
    'symbol' alanı Company tablosuna 1'e 1 ilişki ile bağlıdır.
    """
    symbol = pw.ForeignKeyField(Company, backref='price', unique=True, primary_key=True, on_delete='CASCADE')
    
    price = pw.FloatField(null=True)
    previousClose = pw.FloatField(null=True)
    open = pw.FloatField(null=True)
    high = pw.FloatField(null=True)
    low = pw.FloatField(null=True)
    volume = pw.BigIntegerField(null=True) # Hacimler büyük olabilir
    
    # Son başarılı güncelleme zamanı
    timestamp = pw.DateTimeField(default=datetime.now)
    
    # Veri çekme hatası olursa buraya yazılır
    error = pw.CharField(max_length=255, null=True)

    class Meta:
        # Fiyat verilerini en son güncellenene göre sıralamak için
        ordering = ['timestamp']

# --- Tabloları Oluşturma Fonksiyonu ---
def create_tables():
    """
    Veritabanı bağlantısını açar ve tabloları (eğer yoksa) oluşturur.
    """
    with db:
        db.create_tables([Company, Price])

# Bu dosya doğrudan çalıştırılırsa tabloları oluştursun
if __name__ == '__main__':
    print("Veritabanı tabloları oluşturuluyor...")
    create_tables()
    print("Tablolar başarıyla oluşturuldu (veya zaten mevcuttu).")
    print("Veritabanı dosyası: 'sanbist.db'")
