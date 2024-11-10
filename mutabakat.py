import csv
import os
import glob
import pandas as pd
from zipfile import ZipFile
from datetime import datetime, timedelta

pd.set_option('future.no_silent_downcasting', True)
today = datetime.now()
if today.weekday() == 0:  # Pazartesi günü için weekday() değeri 0'dır
    days_to_subtract = 3
else:
    days_to_subtract = 1

previous_date = today - timedelta(days=days_to_subtract)
previous_date_str = previous_date.strftime("%Y_%m_%d")
previous_date_str_2 = previous_date.strftime("%Y%m%d") #tarihin _'siz hali

# Kybele stok yolu
file_pattern = f"Q:/_HiSenetl/GENEL_MUTABAKAT/IYM/Gunluk_Stok_Raporlari_{previous_date_str}_*.xlsx"
matching_files = glob.glob(file_pattern)
file_to_read = matching_files[0]


#MKK dosyalarını zip'ten çıkarmak
zip_folder = "Q:/_HiSenetl/GENEL_MUTABAKAT/MKK"
extract_folder = "./dosyalar"
for file in os.listdir(zip_folder):
    with ZipFile(os.path.join(zip_folder, file), 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
csv_folder = "./dosyalar/"

#MKK Dosyaları birleştirip filtrelemek
combined_df = pd.concat([pd.read_csv(os.path.join(csv_folder, file), sep=';', thousands=".", decimal=",", low_memory=False) for file in os.listdir(csv_folder)], ignore_index=True)
combined_df.columns=["BAKIYE_TARIHI","UYE","HESAP","ALT_HESAP","KIYMET_SINIFI","MIC","ISIN","EK_TANIM","BAKIYE"]
combined_df['HESAP'] = combined_df['HESAP'].astype(str)
combined_df['HESAP'] = combined_df['HESAP'].fillna('')
filtered_df = combined_df[(combined_df['HESAP'].str.len() < 8) & (~combined_df['HESAP'].str.contains('B|AKHATAP'))]

#MKK Toplam almak ve yazdırmak
sum_by_ISIN=filtered_df.groupby(["ISIN","EK_TANIM"]).agg({"KIYMET_SINIFI":"first","ISIN":"first","EK_TANIM":"first","BAKIYE":"sum"})

#rp055 - borsa kodu dosyası
rp055 = pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/RP055/RP055IYM.csv", encoding="ISO-8859-1", sep=';', header=0)
rp055.columns =["ISIN", "ISIN_ACIKLAMA", "TERTIP_GRUBU", "KIYMET_SINIFI", "KIYMET_TURU", 
			 "IHRACCI_UYE_KODU", "IHRACCI_UYE_ADI", "BORSA_KODU", "TAKAS_KODU", "TAKAS_GRUP_KODU",
			   "ALO_MKK_KODU", "KIYMET_YASAKLILIK_DURUMU", "KAYITLI_SERMAYE_TAVANI",
				"SERMAYE_TURU", "CIKARILMIS_ODENMIS_SERMAYE", "IVR_KODU", "YYF_KURUCUSU", "LEI" ]
rp055["TAKAS_GRUP_KODU"] = rp055["TAKAS_GRUP_KODU"].replace("ESKÝ", "ESKİ")

#MKK dosyasındaki Ek tanım "A" ise rp055deki Takas Kodunu getirmek
df_a=sum_by_ISIN[(sum_by_ISIN["EK_TANIM"]=="A") & (sum_by_ISIN["KIYMET_SINIFI"] != "DB") & (sum_by_ISIN["KIYMET_SINIFI"] != "OST") & (sum_by_ISIN["KIYMET_SINIFI"] != "VDK") & (sum_by_ISIN["KIYMET_SINIFI"] != "Fon") & (sum_by_ISIN["KIYMET_SINIFI"] != "VR") & (sum_by_ISIN["KIYMET_SINIFI"] != "BYF")]
df_a.reset_index(inplace=True, drop=True)
df_a=pd.merge(df_a,rp055[["ISIN","TAKAS_KODU"]],how="left",on="ISIN")
df_A_ = df_a[~df_a['ISIN'].str.startswith('TRR')]

# "TAKAS_KODU" sütununun sonuna "-R" ekle
df_trr=df_a[df_a['ISIN'].str.startswith('TRR')]
df_trr['TAKAS_KODU_R'] = df_trr['TAKAS_KODU'] + '-R'
df_trr['TAKAS_KODU']=df_trr['TAKAS_KODU_R']
df_trr.drop(columns=['TAKAS_KODU_R'], inplace=True)

#Değilse MKK dosyasındaki Kıymet Sınıfı "HS" ve rp055deki Takas Grup Kodu "ESKI" ise
df_hs = sum_by_ISIN[(sum_by_ISIN["EK_TANIM"] != "A") & (sum_by_ISIN["KIYMET_SINIFI"] == "HS")]
df_hs.reset_index(inplace=True, drop=True)
rp055_eski = rp055[rp055["TAKAS_GRUP_KODU"]=="ESKİ"]
merged_df = pd.merge(df_hs, rp055_eski[['ISIN', 'TAKAS_KODU']], on='ISIN', how='inner')
merged_df['New_Column'] = merged_df['TAKAS_KODU'] + " " + merged_df['EK_TANIM']
merged_df['TAKAS_KODU'] = merged_df['New_Column']
merged_df.drop(columns=['New_Column'], inplace=True)
merged_df.reset_index(inplace=True, drop=True)

#Değilse İmtiyazlı kıymetlerin kodunu oluşturmak
df_im=sum_by_ISIN[(sum_by_ISIN["EK_TANIM"] != "A") & (sum_by_ISIN["KIYMET_SINIFI"] == "HS")]
df_im.reset_index(inplace=True, drop=True)
rp055_im=rp055[rp055["TAKAS_GRUP_KODU"]!="ESKİ"]
merged_df_im = pd.merge(df_im, rp055_im[['ISIN', 'TAKAS_KODU']], on='ISIN', how='inner')
merged_df_im['New_Column'] = merged_df_im['TAKAS_KODU'] + " " + merged_df_im['EK_TANIM'] + " "+"Imtiyazli"
merged_df_im['TAKAS_KODU'] = merged_df_im['New_Column']
merged_df_im.drop(columns=['New_Column'], inplace=True)
merged_df_im.reset_index(inplace=True, drop=True)

df_final=pd.concat([df_A_, df_trr, merged_df, merged_df_im],ignore_index=True)
df_final.rename(columns={"TAKAS_KODU" : "Kiymet Kodu"},inplace=True,)

#Kybele stoğunun okunması
kybele_stok = pd.read_excel(file_to_read)
kybele_stok['Kiymet Kodu'] = kybele_stok['Kiymet Kodu'].str.replace('İmtiyazlı', 'Imtiyazli')

#Kybele ve MKK daki kıymetlerin birleştirilerek tekrar edenlerin kaldırılması
merged_kiymet_kodu = pd.concat([kybele_stok['Kiymet Kodu'], df_final['Kiymet Kodu']], ignore_index=True)
merged_kkodu_no_duplicates = merged_kiymet_kodu.drop_duplicates()

#Kıymet koduna göre kybele adedinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, kybele_stok[['Kiymet Kodu', 'Toplam Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates["Toplam Adet"] = merged_kkodu_no_duplicates["Toplam Adet"].fillna(0)
merged_kkodu_no_duplicates.rename(columns={"Toplam Adet" : "Kybele Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="PAY")
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, fark_tablosu[['Kiymet Kodu', 'Kybele Adedi_f']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates["Kybele Adedi_f"] = merged_kkodu_no_duplicates["Kybele Adedi_f"].fillna(0)
merged_kkodu_no_duplicates["Kybele Adedi"]= merged_kkodu_no_duplicates["Kybele Adedi"] + merged_kkodu_no_duplicates["Kybele Adedi_f"]
merged_kkodu_no_duplicates.drop(["Kybele Adedi_f"], axis=1, inplace=True, errors='ignore')

#Kıymet koduna göre MKK adedinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, df_final[['Kiymet Kodu', 'BAKIYE']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"BAKIYE" : "MKK Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="PAY")
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, fark_tablosu[['Kiymet Kodu', 'MKK Adedi_f']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates["MKK Adedi_f"] = merged_kkodu_no_duplicates["MKK Adedi_f"].fillna(0)
merged_kkodu_no_duplicates["MKK Adedi"]= merged_kkodu_no_duplicates["MKK Adedi"] + merged_kkodu_no_duplicates["MKK Adedi_f"]
merged_kkodu_no_duplicates.drop(["MKK Adedi_f"], axis=1, inplace=True, errors='ignore')

#Takas kapalı payların okunması
takas_kapali_paylar = pd.read_csv("Q:/_HiSenetl/_PARYA/TAKAS/TAKAS_INDIRILEN_DOSYALAR/altHesBakHesRap_"+previous_date_str_2+".csv", encoding="ISO-8859-1", low_memory=False)
takas_kapali_paylar.columns = ['ÜyeKod', 'Müşteri No', 'Tanim', 'Grup', 'Hesap', 'Adet', 'Tutar', 'Sözleşme Türü']
filtered_takas_kapali_paylar = takas_kapali_paylar[takas_kapali_paylar['ÜyeKod'].astype(str).str.len() < 8]
grouped_takas_kapali_paylar = filtered_takas_kapali_paylar.groupby('Müşteri No').agg({'Tanim': 'first','Grup': 'first','Hesap': 'sum'}).copy()
grouped_takas_kapali_paylar.reset_index(inplace=True)
grouped_takas_kapali_paylar.rename(columns={"Hesap": "Adet"}, inplace=True)
grouped_takas_kapali_paylar.rename(columns={"Grup": "Hesap"}, inplace=True)
grouped_takas_kapali_paylar.rename(columns={"Tanim": "Grup"}, inplace=True)
grouped_takas_kapali_paylar.rename(columns={"Müşteri No": "Kiymet Kodu"}, inplace=True)

#Kıymet koduna göre Takas Kapalı Pay adedinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, grouped_takas_kapali_paylar[['Kiymet Kodu', 'Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Adet" : "Takas Kapali Paylar Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="PAY")
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, fark_tablosu[['Kiymet Kodu', 'Takas Kapali Paylar Adedi_f']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates["Takas Kapali Paylar Adedi_f"] = merged_kkodu_no_duplicates["Takas Kapali Paylar Adedi_f"].fillna(0)
merged_kkodu_no_duplicates["Takas Kapali Paylar Adedi"]= merged_kkodu_no_duplicates["Takas Kapali Paylar Adedi"] + merged_kkodu_no_duplicates["Takas Kapali Paylar Adedi_f"]
merged_kkodu_no_duplicates.drop(["Takas Kapali Paylar Adedi_f"], axis=1, inplace=True, errors='ignore')

#Viop teminat bakiyelerinin okunması
viop_bakiye = pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/VIOP_TEMINAT_PAY/TeminatBakiyesiRaporu-"+previous_date_str_2+".csv", encoding="ISO-8859-1", low_memory=False)
viop_bakiye['Teminat Kodu'] = viop_bakiye['Teminat Kodu'].str.replace('.COL', '')
viop_bakiye = viop_bakiye.groupby('Teminat Kodu').agg({'Teminat Kodu': 'first','Teminat Tipi': 'first',' Teminat Adedi': 'sum'}).reset_index(drop=True)

#Anlık teminat Bakiyesinin Okunması ve Eklenmesi
anlik_teminat=pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/VIOP_TEMINAT_PAY/AnlıkTeminatBakiyeRaporu-"+previous_date_str_2+".csv", encoding="ISO-8859-1", low_memory=False)
anlik_teminat['Teminat Kodu'] = anlik_teminat['Teminat Kodu'].str.replace('.COL', '')
anlik_teminat= anlik_teminat[anlik_teminat['Teminat Tipi'] == 'OFFSET']
anlik_teminat = anlik_teminat.groupby('Teminat Kodu').agg({'Teminat Kodu': 'first','Teminat Tipi': 'first','Adet': 'sum'}).reset_index(drop=True)
viop_bakiye = pd.merge(viop_bakiye, anlik_teminat, on='Teminat Kodu', how='outer', suffixes=('_viop', '_anlik'))

# 'Teminat Adedi' ve 'Adet' sütunlarını toplaması
viop_bakiye['Toplam Adet'] = viop_bakiye[' Teminat Adedi'].fillna(0) + viop_bakiye['Adet'].fillna(0)
viop_bakiye.rename(columns={" Teminat Adedi": "Viop Adedi"}, inplace=True)
viop_bakiye.rename(columns={"Teminat Kodu": "Kiymet Kodu"}, inplace=True)
viop_bakiye.rename(columns={"Toplam Adet": "Teminat Adedi"}, inplace=True)

#Kıymet koduna göre viop teminat bakiyelerinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, viop_bakiye[['Kiymet Kodu', 'Teminat Adedi']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Teminat Adedi" : "VIOP Teminat Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="PAY")
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, fark_tablosu[['Kiymet Kodu', 'VIOP Teminat Adedi_f']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates["VIOP Teminat Adedi_f"] = merged_kkodu_no_duplicates["VIOP Teminat Adedi_f"].fillna(0)
merged_kkodu_no_duplicates["VIOP Teminat Adedi"]= merged_kkodu_no_duplicates["VIOP Teminat Adedi"] + merged_kkodu_no_duplicates["VIOP Teminat Adedi_f"]
merged_kkodu_no_duplicates.drop(["VIOP Teminat Adedi_f"], axis=1, inplace=True, errors='ignore')

#Tpp verilerinin okunması
csv_file_path = "Q:/_HiSenetl/GENEL_MUTABAKAT/TPP_TEMINAT/TeminatTakipDetayıRaporu-"+previous_date_str_2+".csv"
data_rows = []
start_appending=False

with open(csv_file_path, encoding='latin-1') as file:
    for line in file:
        columns = line.strip().split(',')
        if columns[0].startswith('Teminat Tipi'):
            start_appending = True
        if start_appending:
            data_rows.append(columns)
        
tpp_veri = pd.DataFrame(data_rows[1:], columns=data_rows[0])
headers = ["Teminat Tipi", "Kıymet Tanımı", "Adı", "Adet", "Fiyat", "Piyasa Değeri", "Değerleme Katsayısı", "Değerlenmiş Teminat", "Kullanılabilir Teminat"]
tpp_veri.columns = headers
tpp_veri["Kıymet Tanımı"]=tpp_veri["Kıymet Tanımı"].replace("_E", "", regex=True)
tpp_veri_final = tpp_veri[tpp_veri["Kıymet Tanımı"].apply(lambda x: len(str(x)) <= 5)].copy()
tpp_veri_final.rename(columns={"Kıymet Tanımı": "Kiymet Kodu"}, inplace=True)

#Kıymet koduna göre tpp verilerinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, tpp_veri_final[['Kiymet Kodu', 'Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Adet" : "TPP Teminat Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="PAY")
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, fark_tablosu[['Kiymet Kodu', 'TPP Teminat Adedi_f']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates["TPP Teminat Adedi_f"] = merged_kkodu_no_duplicates["TPP Teminat Adedi_f"].fillna(0)
merged_kkodu_no_duplicates['TPP Teminat Adedi'] = pd.to_numeric(merged_kkodu_no_duplicates['TPP Teminat Adedi'], errors='coerce')
merged_kkodu_no_duplicates['TPP Teminat Adedi_f'] = pd.to_numeric(merged_kkodu_no_duplicates['TPP Teminat Adedi_f'], errors='coerce')
merged_kkodu_no_duplicates["TPP Teminat Adedi"]= merged_kkodu_no_duplicates["TPP Teminat Adedi"] + merged_kkodu_no_duplicates["TPP Teminat Adedi_f"]
merged_kkodu_no_duplicates.drop(["TPP Teminat Adedi_f"], axis=1, inplace=True, errors='ignore')

#Takas kredi teminat verilerinin okunması
file_path="Q:/_HiSenetl/GENEL_MUTABAKAT/TAKAS_KREDI_TEMINAT/TAKAS_KREDI_TEMINAT-"+previous_date_str_2+".csv"
kredi_teminat = pd.read_csv(file_path, encoding = "ISO-8859-1", low_memory=False)
kredi_teminat.reset_index(inplace=True)
kredi_teminat.columns =["MusteriNo","Müşteri Tip","Müşteri Kod","TeminatTipi",
    "TeminatKodu","Kıymet Grubu","Adet","Fiyat","PiyasaDeğeri","Değerleme Katsayısı",
    "DeğerlenmişTutar","ÇözülebilecekAdet","Kullanılabilir Değ.Teminat", "BulundurulmasıGerekenTeminat", "Risk", "A"]
kredi_teminat_sum=kredi_teminat.groupby(["TeminatKodu"]).agg({"TeminatKodu":"first","Adet":"sum"})
kredi_teminat_sum.rename(columns={"TeminatKodu": "Kiymet Kodu"}, inplace=True)

#Kıymet koduna göre takas kredi teminat adetlerinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, kredi_teminat_sum[['Kiymet Kodu', 'Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Adet" : "Takas Kredi Teminat Adedi"},inplace=True,)

#Bistech eq teminat verilerinin okunması
bistech_eq = pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/MKT_TEMINAT_EQ/EQ-"+previous_date_str_2+".csv", encoding="ISO-8859-1", sep=';', header=0)
bistech_eq.columns =["Hesap", "Harici Hesap", "Menkul", "Geçici Nakit Teminat Bakiyesi", "Geçici Menkul Kıymet Teminatı Bakiyesi", "Tutar", "Nakit Teminat", "A"]
bistech_eq['Menkul'] = bistech_eq['Menkul'].str.replace('.COL', '')
bistech_eq['Geçici Menkul Kıymet Teminatı Bakiyesi'] = bistech_eq['Geçici Menkul Kıymet Teminatı Bakiyesi'].str.replace('.', '').str.replace(',', '.').astype(float)
bistech_eq_grouped= bistech_eq.groupby('Menkul').agg({
   'Menkul': 'first','Geçici Menkul Kıymet Teminatı Bakiyesi': 'sum'})
bistech_eq_grouped.rename(columns={"Menkul": "Kiymet Kodu"}, inplace=True)
bistech_eq_grouped.rename(columns={"Geçici Menkul Kıymet Teminatı Bakiyesi": "Adet"}, inplace=True)

#Kıymet koduna göre bistech eq teminat adetlerinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, bistech_eq_grouped[['Kiymet Kodu', 'Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Adet" : "Bistech EQ Teminat Adedi"},inplace=True,)

#Bistech fi teminat verilerinin okunması
bistech_fi = pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/MKT_TEMINAT_FI/FI-"+previous_date_str_2+".csv", encoding="ISO-8859-1", sep=';', header=0)
bistech_fi.columns =["Hesap", "Harici Hesap Numarası", "Menkul", "Geçici Nakit Teminat Bakiyesi", "Geçici Menkul Kıymet Teminatı Bakiyesi", "Tutar", "Nakit Teminat","A"]
bistech_fi['Menkul'] = bistech_fi['Menkul'].str.replace('.COL', '')
bistech_fi['Geçici Menkul Kıymet Teminatı Bakiyesi'] = bistech_fi['Geçici Menkul Kıymet Teminatı Bakiyesi'].str.replace('.', '').str.replace(',', '.').astype(float)
bistech_fi_grouped= bistech_fi.groupby('Menkul').agg({
   'Menkul': 'first','Geçici Menkul Kıymet Teminatı Bakiyesi': 'sum'})
bistech_fi_grouped.rename(columns={"Menkul": "Kiymet Kodu"}, inplace=True)
bistech_fi_grouped.rename(columns={"Geçici Menkul Kıymet Teminatı Bakiyesi": "Adet"}, inplace=True)

#Kıymet koduna göre bistech fi teminat adetlerinin getirilmesi
merged_kkodu_no_duplicates = pd.merge(merged_kkodu_no_duplicates, bistech_fi_grouped[['Kiymet Kodu', 'Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Adet" : "Bistech FI Teminat Adedi"},inplace=True,)

#Saklamacı kurumlardaki kıymetlerinin toplamının alınması
numeric_columns = [ 'MKK Adedi', 'Takas Kapali Paylar Adedi',
                   'VIOP Teminat Adedi', 'TPP Teminat Adedi',
                   'Takas Kredi Teminat Adedi', 'Bistech EQ Teminat Adedi',
                   'Bistech FI Teminat Adedi']
merged_kkodu_no_duplicates[numeric_columns] = merged_kkodu_no_duplicates[numeric_columns].replace(',', '', regex=True).astype(float)
merged_kkodu_no_duplicates['Total'] = merged_kkodu_no_duplicates[numeric_columns].sum(axis=1)

#Kybele ile saklamacı kurum farkının alınması
merged_kkodu_no_duplicates['FARK'] = merged_kkodu_no_duplicates['Kybele Adedi'] - merged_kkodu_no_duplicates['Total']

#Mutabakat dosyasına fiyatların getirilmesi
kybele_fiyat = pd.read_excel(file_to_read,sheet_name="Pay Fiyat")
merged_kkodu_no_duplicates= pd.merge(merged_kkodu_no_duplicates, kybele_fiyat[['Kiymet Kodu', 'Kapanis']], on='Kiymet Kodu', how='left')

#Other_kapanis = merged_kkodu_no_duplicates[~merged_kkodu_no_duplicates['Kiymet Kodu'].str.contains('Imtiyazli')]
other_kapanis = merged_kkodu_no_duplicates[~merged_kkodu_no_duplicates['Kiymet Kodu'].astype(str).str.contains('Imtiyazli')]

for index, row in other_kapanis.iterrows():
    if pd.isna(row['Kapanis']) or row['Kapanis'] == '':
        kiymet_kodu = str(row['Kiymet Kodu'])  # Convert to string
        prefix = kiymet_kodu[:4]
        mask = other_kapanis['Kiymet Kodu'].astype(str).str.startswith(prefix)  # Ensure Kiymet Kodu is treated as string
        non_empty_values = other_kapanis.loc[mask & (other_kapanis['Kapanis'] != '')]['Kapanis']
        if len(non_empty_values) > 0:
            other_kapanis.at[index, 'Kapanis'] = non_empty_values.iloc[0] 
merged_kkodu_no_duplicates.update(other_kapanis)

#Toplam piyasa değerinin bulunması
merged_kkodu_no_duplicates["Toplam Piyasa Degeri"]=merged_kkodu_no_duplicates["Kybele Adedi"] * merged_kkodu_no_duplicates["Kapanis"]

#Kıymetlerin hesap bazlı dağılımı
kybele_hesap = pd.read_excel(file_to_read,sheet_name="Pay Kıymet")
merged_kkodu_no_duplicates= pd.merge(merged_kkodu_no_duplicates, kybele_hesap[['Kiymet Kodu', 'Adet']], on='Kiymet Kodu', how='left')
merged_kkodu_no_duplicates.rename(columns={"Adet" : "Hesap Adedi"},inplace=True,)
columns_to_convert = [ 'MKK Adedi', 'Takas Kapali Paylar Adedi', 
                      'VIOP Teminat Adedi', 'TPP Teminat Adedi', 
                      'Takas Kredi Teminat Adedi', 'Bistech EQ Teminat Adedi', 
                      'Bistech FI Teminat Adedi', 'Total', 'Kapanis', 
                      'Toplam Piyasa Degeri']

merged_kkodu_no_duplicates[columns_to_convert] = merged_kkodu_no_duplicates[columns_to_convert].fillna(0)
merged_kkodu_no_duplicates.to_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx", float_format='%.3f',sheet_name='Pay Mutabakat Raporu', index=False)



#VARANT
#MKK varant stoğunun filtrelenmesi
mkk_varant=sum_by_ISIN[sum_by_ISIN["KIYMET_SINIFI"]=="VR"].copy()
mkk_varant.reset_index(inplace=True, drop=True)
mkk_varant.rename(columns={"ISIN" : "ISINCode"},inplace=True,)

#Kybele varant stoğunun okunması
kybele_varant_stok= pd.read_excel(file_to_read,sheet_name="Varant Özet")

#Kybele ve MKK daki kıymetlerin birleştirilerek tekrar edenlerin kaldırılması
merged_varant = pd.concat([kybele_varant_stok['ISINCode'], mkk_varant['ISINCode']], ignore_index=True)
merged_varant_no_duplicates = merged_varant.drop_duplicates()

#Kıymet koduna göre kybele adedinin getirilmesi
merged_varant_no_duplicates = pd.merge(merged_varant_no_duplicates,kybele_varant_stok [['ISINCode', 'Toplam Adet']], on='ISINCode', how='left')
merged_varant_no_duplicates ["Toplam Adet"] = merged_varant_no_duplicates ["Toplam Adet"].fillna(0)
merged_varant_no_duplicates .rename(columns={"Toplam Adet" : "Kybele Adedi"},inplace=True,)

#Kıymet koduna göre MKK adedinin getirilmesi
merged_varant_no_duplicates = pd.merge(merged_varant_no_duplicates, mkk_varant[['ISINCode', 'BAKIYE']], on='ISINCode', how='left')
merged_varant_no_duplicates ["BAKIYE"] = merged_varant_no_duplicates ["BAKIYE"].fillna(0)
merged_varant_no_duplicates.rename(columns={"BAKIYE" : "MKK Adedi"},inplace=True)

#Kybele ile saklamacı kurum farkının alınması
merged_varant_no_duplicates['FARK'] = merged_varant_no_duplicates['Kybele Adedi'] - merged_varant_no_duplicates['MKK Adedi']

#Mutabakat dosyasına fiyatların getirilmesi
kybele_varant_fiyat = pd.read_excel(file_to_read,sheet_name="Varant Fiyat")
merged_varant_no_duplicates= pd.merge(merged_varant_no_duplicates, kybele_varant_fiyat[['ISINCode', 'Kapanis']], on='ISINCode', how='left')

#Toplam piyasa değerinin bulunması
merged_varant_no_duplicates["Toplam Piyasa Degeri"]=merged_varant_no_duplicates["Kybele Adedi"] * merged_varant_no_duplicates["Kapanis"]

#Kıymetlerin hesap bazlı dağılımı
kybele_varant_hesap = pd.read_excel(file_to_read,sheet_name="Varant Kıymet")
merged_varant_no_duplicates= pd.merge(merged_varant_no_duplicates, kybele_varant_hesap[['ISINCode', 'Adet']], on='ISINCode', how='left')
merged_varant_no_duplicates.rename(columns={"Adet" : "Hesap Adedi"},inplace=True,)

#Verilerin STOK MUTABAKAT RAPORU dosyasına yazdırılması
if os.path.exists("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx"):
    with pd.ExcelWriter("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx", engine='openpyxl', mode='a') as writer:
        merged_varant_no_duplicates.to_excel(writer, sheet_name='Varant Mutabakat Raporu', index=False)



#BYF
#MKK byf stoğunun filtrelenmesi
mkk_byf=sum_by_ISIN[sum_by_ISIN["KIYMET_SINIFI"]=="BYF"].copy()
mkk_byf.reset_index(inplace=True, drop=True)
mkk_byf.rename(columns={"ISIN" : "ISINCode"},inplace=True,)

#Kybele byf stoğunun okunması
kybele_byf_stok = pd.read_excel(file_to_read, sheet_name="BYF Özet")

#Kybele ve MKK daki kıymetlerin birleştirilerek tekrar edenlerin kaldırılması
merged_byf = pd.concat([kybele_byf_stok['ISINCode'], mkk_byf['ISINCode']], ignore_index=True)
merged_byf_no_duplicates = merged_byf.drop_duplicates()

#Kıymet koduna göre kybele adedinin getirilmesi
merged_byf_no_duplicates = pd.merge(merged_byf_no_duplicates,kybele_byf_stok [['ISINCode', 'Toplam Adet']], on='ISINCode', how='left')
merged_byf_no_duplicates ["Toplam Adet"] = merged_byf_no_duplicates ["Toplam Adet"].fillna(0)
merged_byf_no_duplicates .rename(columns={"Toplam Adet" : "Kybele Adedi"},inplace=True,)

#Kıymet koduna göre MKK adedinin getirilmesi
merged_byf_no_duplicates = pd.merge(merged_byf_no_duplicates, mkk_byf[['ISINCode', 'BAKIYE']], on='ISINCode', how='left')
merged_byf_no_duplicates ["BAKIYE"] = merged_byf_no_duplicates["BAKIYE"].fillna(0)
merged_byf_no_duplicates.rename(columns={"BAKIYE" : "MKK Adedi"},inplace=True)

#Kybele ile saklamacı kurum farkının alınması
merged_byf_no_duplicates['FARK'] = merged_byf_no_duplicates['Kybele Adedi'] - merged_byf_no_duplicates['MKK Adedi']

#Mutabakat dosyasına fiyatların getirilmesi
kybele_byf_fiyat = pd.read_excel(file_to_read,sheet_name="BYF Fiyat")
merged_byf_no_duplicates= pd.merge(merged_byf_no_duplicates, kybele_byf_fiyat[['ISINCode', 'Kapanis']], on='ISINCode', how='left')

#Toplam piyasa değerinin bulunması
merged_byf_no_duplicates["Toplam Piyasa Degeri"]=merged_byf_no_duplicates["Kybele Adedi"] * merged_byf_no_duplicates["Kapanis"]

#Kıymetlerin hesap bazlı dağılımı
kybele_byf_hesap = pd.read_excel(file_to_read,sheet_name="BYF Kıymet")
merged_byf_no_duplicates= pd.merge(merged_byf_no_duplicates, kybele_byf_hesap[['ISINCode', 'Adet']], on='ISINCode', how='left')
merged_byf_no_duplicates.rename(columns={"Adet" : "Hesap Adedi"},inplace=True,)

#Verilerin STOK MUTABAKAT RAPORU dosyasına yazdırılması
if os.path.exists("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx"):
    with pd.ExcelWriter("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx", engine='openpyxl', mode='a') as writer:
        merged_byf_no_duplicates.to_excel(writer, sheet_name='BYF Mutabakat Raporu', index=False)




#SGMK
#MKK sgmk stoğunun filtrelenmesi
mkk_sgmk = sum_by_ISIN[(sum_by_ISIN["KIYMET_SINIFI"] == "DB") | (sum_by_ISIN["KIYMET_SINIFI"] == "OST")| (sum_by_ISIN["KIYMET_SINIFI"] == "VDK")].copy()
mkk_sgmk.reset_index(inplace=True, drop=True)
mkk_sgmk.rename(columns={"ISIN": "ISINCode"}, inplace=True)

#Kybele sgmk stoğunun okunması
kybele_sgmk_stok = pd.read_excel(file_to_read, sheet_name="SGMK Özet")
kybele_sgmk_stok_sum = kybele_sgmk_stok.groupby(["ISINCode"]).agg({"ISINCode":"first",'Toplam Adet':"sum"}).copy()
kybele_sgmk_stok_sum = kybele_sgmk_stok_sum.reset_index(drop=True)

#Kybele ve MKK daki kıymetlerin birleştirilerek tekrar edenlerin kaldırılması
merged_sgmk = pd.concat([kybele_sgmk_stok['ISINCode'], mkk_sgmk['ISINCode']], ignore_index=True)
merged_sgmk_no_duplicates = merged_sgmk.drop_duplicates()

#Kıymet koduna göre kybele adedinin getirilmesi
merged_sgmk_no_duplicates = pd.merge(merged_sgmk_no_duplicates, kybele_sgmk_stok[['ISINCode', 'Toplam Adet']], on='ISINCode', how='left')
merged_sgmk_no_duplicates['Toplam Adet'] = merged_sgmk_no_duplicates['Toplam Adet'].fillna(0)
merged_sgmk_no_duplicates .rename(columns={'Toplam Adet' : "Kybele Adedi"},inplace=True,)

#Kıymet koduna göre MKK adedinin getirilmesi
merged_sgmk_no_duplicates = pd.merge(merged_sgmk_no_duplicates, mkk_sgmk[['ISINCode', 'BAKIYE']], on='ISINCode', how='left')
merged_sgmk_no_duplicates ["BAKIYE"] = merged_sgmk_no_duplicates["BAKIYE"].fillna(0)
merged_sgmk_no_duplicates.rename(columns={"BAKIYE" : "MKK Adedi"},inplace=True)

#Takas Bank SGMK Bakiyelerinin okunması
takas_sgmk=pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/TAKAS_SGMK/uyeSaklamaBakiyeRapor-"+previous_date_str_2+".csv", encoding="ISO-8859-1", low_memory=False)
takas_sgmk.rename(columns={" MkKod": "ISINCode"}, inplace=True)

#Takas Bank SGMK Bakiyelerinin getirilmesi
merged_sgmk_no_duplicates = pd.merge(merged_sgmk_no_duplicates, takas_sgmk[['ISINCode', ' SakTutar']], on='ISINCode', how='left')
merged_sgmk_no_duplicates.rename(columns={" SakTutar" : "Takas SGMK Adedi"},inplace=True,)

#Takas Teminat Bakiyelerinin getirilmesi
teminat_sgmk = pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/MKT_TEMINAT_FI/FI-"+previous_date_str_2+".csv", encoding="ISO-8859-1", sep=';', header=0)
teminat_sgmk.columns =["Hesap", "Harici Hesap Numarası", "Menkul", "Geçici Nakit Teminat Bakiyesi", "Geçici Menkul Kıymet Teminatı Bakiyesi", "Tutar", "Nakit Teminat","A"]
teminat_sgmk['Menkul'] = teminat_sgmk['Menkul'].str.replace('.COL', '')
teminat_sgmk['Geçici Menkul Kıymet Teminatı Bakiyesi'] = teminat_sgmk['Geçici Menkul Kıymet Teminatı Bakiyesi'].str.replace('.', '').str.replace(',', '.').astype(float)
teminat_sgmk_grouped= teminat_sgmk.groupby('Menkul').agg({'Menkul': 'first','Geçici Menkul Kıymet Teminatı Bakiyesi': 'sum'})
teminat_sgmk_grouped.rename(columns={"Menkul": "ISINCode"}, inplace=True)
teminat_sgmk_grouped.rename(columns={"Geçici Menkul Kıymet Teminatı Bakiyesi": "Adet"}, inplace=True)

#Kıymet koduna göre Takas Teminat Bakiyelerinin getirilmesi
merged_sgmk_no_duplicates = pd.merge(merged_sgmk_no_duplicates, teminat_sgmk_grouped[['ISINCode', 'Adet']], on='ISINCode', how='left')
merged_sgmk_no_duplicates.rename(columns={"Adet" : "Takas Teminat Adedi"},inplace=True,)

#Saklamacı kurumlardaki kıymetlerinin toplamının alınması
numeric_columns = [ 'MKK Adedi','Takas SGMK Adedi', "Takas Teminat Adedi"]
merged_sgmk_no_duplicates[numeric_columns] = merged_sgmk_no_duplicates[numeric_columns].replace(',', '', regex=True).astype(float)
merged_sgmk_no_duplicates['Total'] = merged_sgmk_no_duplicates[numeric_columns].sum(axis=1)

#Kybele ile saklamacı kurum farkının alınması
merged_sgmk_no_duplicates['FARK'] = merged_sgmk_no_duplicates['Kybele Adedi'] - merged_sgmk_no_duplicates['Total']

#Mutabakat dosyasına fiyatların getirilmesi
kybele_sgmk_fiyat = pd.read_excel(file_to_read,sheet_name="SGMK Fiyat")
merged_sgmk_no_duplicates= pd.merge(merged_sgmk_no_duplicates, kybele_sgmk_fiyat[['ISINCode', 'Agirlikli Ort']], on='ISINCode', how='left')

#Toplam piyasa değerinin bulunması
merged_sgmk_no_duplicates["Toplam Piyasa Degeri"]=merged_sgmk_no_duplicates["Kybele Adedi"] * merged_sgmk_no_duplicates["Agirlikli Ort"]

#Kıymetlerin hesap bazlı dağılımı
kybele_sgmk_hesap = pd.read_excel(file_to_read,sheet_name="SGMK Kıymet")
merged_sgmk_no_duplicates= pd.merge(merged_sgmk_no_duplicates, kybele_sgmk_hesap[['ISINCode', 'Adet']], on='ISINCode', how='left')
merged_sgmk_no_duplicates.rename(columns={"Adet" : "Hesap Adedi"},inplace=True,)

#Verilerin STOK MUTABAKAT RAPORU dosyasına yazdırılması
if os.path.exists("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx"):
    with pd.ExcelWriter("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx", engine='openpyxl', mode='a') as writer:
        merged_sgmk_no_duplicates.to_excel(writer, sheet_name='SGMK Mutabakat Raporu', index=False)



#FON
#MKK fon stoğunun filtrelenmesi
mkk_fon=sum_by_ISIN[sum_by_ISIN["KIYMET_SINIFI"]=="Fon"].copy()
mkk_fon.reset_index(inplace=True, drop=True)
mkk_fon.rename(columns={"ISIN" : "ISINCode"},inplace=True,)

#Kybele fon stoğunun okunması
kybele_fon_stok = pd.read_excel(file_to_read, sheet_name="Fon Özet")

#Kybele ve MKK daki kıymetlerin birleştirilerek tekrar edenlerin kaldırılması
merged_fon = pd.concat([kybele_fon_stok[['ISINCode', 'EK_TANIM']], mkk_fon[['ISINCode', 'EK_TANIM']]], ignore_index=True)
merged_fon_no_duplicates = merged_fon.drop_duplicates()
merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates,kybele_fon_stok[['ISINCode', 'EK_TANIM', 'Kiymet Kodu']],on=['ISINCode', 'EK_TANIM'],how='left')

#Kıymet koduna göre kybele adedinin getirilmesi
merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates, kybele_fon_stok[['ISINCode', 'EK_TANIM', 'Kiymet Kodu', 'Toplam Adet']], on=['ISINCode', 'EK_TANIM', 'Kiymet Kodu'], how='left')
merged_fon_no_duplicates ["Toplam Adet"] = merged_fon_no_duplicates ["Toplam Adet"].fillna(0)
merged_fon_no_duplicates .rename(columns={"Toplam Adet" : "Kybele Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="FON")
fark_tablosu = fark_tablosu.groupby(['ISINCode', 'EK_TANIM']).agg({'ISINCode': 'first','EK_TANIM': 'first','Kybele Adedi_f': 'sum'})
fark_tablosu = fark_tablosu.reset_index(drop=True)

merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates, fark_tablosu[['ISINCode', 'EK_TANIM', 'Kybele Adedi_f']],on=['ISINCode', 'EK_TANIM'], how='left')
merged_fon_no_duplicates["Kybele Adedi_f"] = merged_fon_no_duplicates["Kybele Adedi_f"].fillna(0)
merged_fon_no_duplicates["Kybele Adedi"]= merged_fon_no_duplicates["Kybele Adedi"] + merged_fon_no_duplicates["Kybele Adedi_f"]
merged_fon_no_duplicates.drop(["Kybele Adedi_f"], axis=1, inplace=True, errors='ignore')
merged_fon_no_duplicates = merged_fon_no_duplicates.drop_duplicates()

#Kıymet koduna göre MKK adedinin getirilmesi
merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates, mkk_fon[['ISINCode', 'EK_TANIM','BAKIYE']], on=['ISINCode', 'EK_TANIM'], how='left')
merged_fon_no_duplicates ["BAKIYE"] = merged_fon_no_duplicates["BAKIYE"].fillna(0)
merged_fon_no_duplicates.rename(columns={"BAKIYE" : "MKK Adedi"},inplace=True)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="FON")
cols_to_fill = fark_tablosu.columns.difference(['ISINCode', 'FARK AÇIKLAMA'])
fark_tablosu[cols_to_fill] = fark_tablosu[cols_to_fill].fillna(0)
columns_to_sum = ['MKK Adedi_f', 'Takas Kapali Paylar Adedi_f', 'TPP Teminat Adedi_f']
total_sum = fark_tablosu[columns_to_sum].sum().sum()
merged_fon_no_duplicates['MKK Adedi'] = merged_fon_no_duplicates['MKK Adedi'] + total_sum

#Viop teminat bakiyelerinin okunması
viop_bakiye = pd.read_csv("Q:/_HiSenetl/GENEL_MUTABAKAT/VIOP_TEMINAT_FON/TeminatBakiyesiRaporu-Fon-"+previous_date_str_2+".csv", encoding="ISO-8859-1", low_memory=False)
viop_bakiye['Teminat Kodu'] = viop_bakiye['Teminat Kodu'].str.replace('.COL', '')
viop_bakiye = viop_bakiye.groupby('Teminat Kodu').agg({
    'Teminat Kodu': 'first','Teminat Tipi': 'first',' Teminat Adedi': 'sum'}).copy()
viop_bakiye.rename(columns={"Teminat Kodu": "Kiymet Kodu"}, inplace=True)

#Kıymet koduna göre viop teminat bakiyelerinin getirilmesi
merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates, viop_bakiye[['Kiymet Kodu', ' Teminat Adedi']], on='Kiymet Kodu', how='left')
merged_fon_no_duplicates.rename(columns={" Teminat Adedi" : "VIOP Teminat Adedi"},inplace=True,)

fark_tablosu = pd.read_excel("Q:/_HiSenetl/GENEL_MUTABAKAT/FARK TABLOSU.xlsx", sheet_name="FON")
fark_tablosu = fark_tablosu.groupby(['ISINCode', 'EK_TANIM']).agg({'ISINCode': 'first','EK_TANIM': 'first','VIOP Teminat Adedi_f': 'sum'})
fark_tablosu = fark_tablosu.reset_index(drop=True)

merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates, fark_tablosu[['ISINCode', 'VIOP Teminat Adedi_f']], on='ISINCode', how='left')
merged_fon_no_duplicates["VIOP Teminat Adedi_f"] = merged_fon_no_duplicates["VIOP Teminat Adedi_f"].fillna(0)
merged_fon_no_duplicates["VIOP Teminat Adedi"]= merged_fon_no_duplicates["VIOP Teminat Adedi"] + merged_fon_no_duplicates["VIOP Teminat Adedi_f"]
merged_fon_no_duplicates.drop(["VIOP Teminat Adedi_f"], axis=1, inplace=True, errors='ignore')

#Saklamacı kurumlardaki kıymetlerinin toplamının alınması
numeric_columns = [ 'MKK Adedi','VIOP Teminat Adedi']
merged_fon_no_duplicates[numeric_columns] = merged_fon_no_duplicates[numeric_columns].replace(',', '', regex=True).astype(float)
merged_fon_no_duplicates['Total'] = merged_fon_no_duplicates[numeric_columns].sum(axis=1)

#Kybele ile saklamacı kurum farkının alınması
merged_fon_no_duplicates['FARK'] = merged_fon_no_duplicates['Kybele Adedi'] - merged_fon_no_duplicates['Total']

#Mutabakat dosyasına fiyatların getirilmesi
kybele_fon_fiyat = pd.read_excel(file_to_read,sheet_name="Fon Fiyat")
merged_fon_no_duplicates= pd.merge(merged_fon_no_duplicates, kybele_fon_fiyat[['ISINCode', 'Agirlikli Ort']], on='ISINCode', how='left')

#Toplam piyasa değerinin bulunması
merged_fon_no_duplicates["Toplam Piyasa Degeri"]=merged_fon_no_duplicates["Kybele Adedi"] * merged_fon_no_duplicates["Agirlikli Ort"]

#Kıymetlerin hesap bazlı dağılımı
kybele_fon_hesap = pd.read_excel(file_to_read,sheet_name="Fon Kıymet")
merged_fon_no_duplicates = pd.merge(merged_fon_no_duplicates, kybele_fon_hesap[['ISINCode', 'EK_TANIM', 'Adet']], on=['ISINCode', 'EK_TANIM'], how='left')
merged_fon_no_duplicates.rename(columns={"Adet" : "Hesap Adedi"},inplace=True,)

#Verilerin STOK MUTABAKAT RAPORU dosyasına yazdırılması
if os.path.exists("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx"):
    with pd.ExcelWriter("Q:/_HiSenetl/GENEL_MUTABAKAT/MUTABAKAT_SONUCLARI/STOK MUTABAKAT RAPORU_"+previous_date_str+".xlsx", engine='openpyxl', mode='a') as writer:
        merged_fon_no_duplicates.to_excel(writer, sheet_name='Fon Mutabakat Raporu', index=False)

        
