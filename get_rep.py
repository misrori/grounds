from dotenv import load_dotenv
import pandas as pd
import requests
import os
from tqdm import tqdm
import json
import pickle
import google.generativeai as genai
from datetime import datetime
import time
from telegram.helpers import escape_markdown
import telebot


load_dotenv()

# summarize with gemini
my_gemini_api_key = os.environ.get('GEMINI')
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')  # foldek chat ID

bot = telebot.TeleBot(BOT_TOKEN)

genai.configure(api_key=my_gemini_api_key)

create_processing_dir = 'processed_data'
if not os.path.exists(create_processing_dir):
    os.makedirs(create_processing_dir)

def read_old_data():
    data_files = os.listdir('processed_data')
    data_files = [f for f in data_files if f.endswith('.pickle')]
    if not data_files:
        return None
    else:
        df_list = []
        for file in data_files:
            file_path = os.path.join('processed_data', file)
            df = pd.read_pickle(file_path)
            df_list.append(df)
        
        # Combine all dataframes into one
        df = pd.concat(df_list, ignore_index=True)
        return df


# ---
## 2. Hirdetmény Azonosítók Lekérdezése (get_eids)
def get_all_eids():
    """
    Lekéri az összes meghirdetett információ (ügyirat) azonosítóját.
    """
    print("\n### 2. Hirdetmény Azonosítók Lekérdezése ###")
    url = 'https://hirdetmenyek.gov.hu/api/hirdetmenyek?order=desc&targy=&kategoria=foldelovasarlasos&forrasIntezmenyNeve=&ugyiratSzamIktatasiSzam=&telepules=&nev=&idoszak=&adottNap=&szo=&pageIndex=0&pageSize=10000&sort=kifuggesztesNapja'
    print(f"Hirdetmény azonosítók lekérdezése az alábbi URL-ről: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status() # Hiba esetén kivételt dob
        df = pd.DataFrame(response.json()['rows'])
        print(f"Sikeresen lekérdezve **{len(df)}** hirdetmény azonosító.")
        print("Az első 5 azonosító:")
        return df
    except requests.exceptions.RequestException as e:
        print(f"**HIBA** történt az azonosítók lekérdezésekor: {e}")
        return pd.DataFrame() # Üres DataFrame-et ad vissza hiba esetén

# Futtatás
new_data = get_all_eids()

old_df = read_old_data()
if old_df is not None:
    # old_df = pd.read_pickle('all_eids.pickle')
    print(f"Régi adatok betöltve: {len(old_df)} azonosító.")
    old_ids = old_df['azonosító'].tolist()
    print(f"Régi azonosítók száma: {len(old_ids)}")
else:
    old_ids = []
    print("Nincsenek régi adatok, új azonosítókat fogunk menteni.")


# check the new ids which are not in the old ids
if not new_data.empty:
    new_ids = new_data[~new_data['id'].isin(old_ids)]
    if not new_ids.empty:
        print(f"Új azonosítók találhatók: {len(new_ids)}")
    else:
        print("Nincsenek új azonosítók.")
        exit(0)


# ---
## 3. Részletes Információk Lekérdezése és Mentése

def get_and_save_detailed_info(e_id):
    time.sleep(1)  # To avoid hitting the API too fast
    """
    Lekéri egy adott hirdetmény részletes információit és JSON formátumban menti.
    """
    url = f'https://hirdetmenyek.gov.hu/api/hirdetmenyek/reszletezo/{e_id}'
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        details = response.json()
        details['id'] = e_id  # hozzáadjuk az azonosítót a részletekhez
        details['reszletek_link'] = f"https://hirdetmenyek.gov.hu/reszletezo/{e_id}"  # link hozzáadása

 
        # print(f"  Részletes infó ({e_id}) elmentve: {json_file_path}") # Ezt most a progress bar-ban látjuk
        return details
    except requests.exceptions.RequestException as e:
        print(f"  **HIBA** történt a részletes infók lekérdezésekor ({e_id}): {e}")
        return None


# get the json data
all_data_json = []

max_to_process =  40

for i in tqdm(new_ids['id'][0:max_to_process]):
    details = get_and_save_detailed_info(i)
    if details:
        all_data_json.append(details)




def get_batch_info_df(temp_data):
    time.sleep(10)  # To avoid hitting the API too fast
    prompt = (
        "Átadok egy listát ami json adatokat tartalmaz minden egyes elemeal istának az egy ingatlan adásvétel vagy bérbeadásáról szól és ö nagyon trükkös maga a jason formú öö struktúra mert ezek különbözők lehetnek szóval van ahol egy darab vétel árban van valahol több van valahol az egyvétel árman szövegként van berakva és el van választva egymással szóval így én nehezen értelmezhető és neked az lenne a feladatod hogy minden egyes elemét annak a listának külön értelmezd és elemezd és az elemzésben a következőket szeretném visszakapni hány darab csatolmány volt a fához mi az azonosítója a linkje az összes vétel árnak az összegét és ezen kívül hogy hungary darab helyrajzi szám darab volt az adásvételi ben illetve hány darab ingatlanról van szó\n"
        "Kérlek, JSON formátumban válaszolj. "
        "A JSON-nek tartalmaznia kell legalább a következő mezőket: "
        "dokumentum_targya, település, Linket a részletekhez, azonosító, csatolmányok száma, vételárak összegzése, helyrajzi számok száma, ingatlanok száma. "
        "a vételárakat amenyiben több is van add össze és csak egy számot válaszolj, szám formátumban"
        "az árat számként válaszold és legyen egy külön pénznem adat is (pl. HUF). "
        "amenyiben haszonbérletről van szó, akkor a vételár helyett a bérleti díjat add meg, "
        "és a pénznem legyen HUF vagy EUR, attól függően, hogy milyen pénznemben van megadva. "

        "több ingatlan esetén a vételár érték tartalmazza az összes szóban forgó ingatlan értékét"
        "csak a json adatát válaszold hogy az utobbi kod müködjön  raw_json_string = response.text.strip()"
        "Példa JSON struktúra: "
        "{\n"
        "  \"dokumentum_targya\": \"dokumentum tárgya\",\n"
        "  \"település\": \"település_ide\",\n"
        "  \"Link a részletekhez\": \"link\",\n"
        "  \"azonosító\": \"hirdetmény azonosító\",\n"
        "  \"csatolmányok száma\": \"csatolmányok_száma számként\",\n"
        "  \"helyrajzi számok száma\": \"helyrajzi számok száma számként\",\n"
        "  \"ingatlanok száma\": \"ingatlanok száma számként\",\n"
        "  \"település\": \"település_ide\",\n"
        "  \"vételár\": \"ár_ide az összes ár összeadva számként\",\n"
        "  \"pénznem\": \"HUF vagy EUR\",\n"
        "}\n\n"
        "JSON:\n" 
    )
    prompt += json.dumps(temp_data, ensure_ascii=False, indent=4) + "\n"

    try:
        print("Gemini API hívása...")
        model = genai.GenerativeModel('gemini-1.5-flash') # Using 1.5 Flash for potentially better instruction following
        response = model.generate_content(prompt)
        raw_json_string = response.text.strip()

        # remove``` jsoon from the beginining and end if present
        # if raw_json_string.startswith('```json'):
        if raw_json_string.startswith('```json'):
            raw_json_string = raw_json_string[8:].strip()
        if raw_json_string.endswith('```'):
            raw_json_string = raw_json_string[:-3].strip()

        # Attempt to parse the string as JSON
        try:
            parsed_json = json.loads(raw_json_string)
            df = pd.DataFrame(parsed_json)
            return df

        except json.JSONDecodeError as e:
            print(f"Hiba a JSON dekódolása során: {e}")
            print(f"Nyers válasz a Geminitől:\n{raw_json_string}")
            print( None)
    except Exception as e:
        print(f"Hiba a Gemini API hívása során: {e}")


# with 20 line batching get the df and combine them
batch_size = 20
all_dfs = []
for i in tqdm(range(0, len(all_data_json), batch_size)):
    batch = all_data_json[i:i + batch_size]
    df = get_batch_info_df(batch)
    if df is not None:
        all_dfs.append(df)
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file_name = f"{create_processing_dir}/hirdetmeny_osszegzes_batch_{current_time}.pickle"
    df.to_pickle(csv_file_name)





    

# Combine all DataFrames into one
if all_dfs:
    final_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Összesített DataFrame mérete: {final_df.shape}")
    # Save the final DataFrame to a pickle file
    final_pickle_path = os.path.join('hirdetmeny_osszegzes_final.pickle')
    final_df.to_pickle(final_pickle_path)
    print(f"Összesített DataFrame elmentve: {final_pickle_path}")




### REPORT IT



# make sure it is markdown format

def send_telegram_message(message, parse_mode="HTML"):
    try:
        bot.send_message(chat_id=CHAT_ID, text=message, parse_mode=parse_mode)
    except Exception as e:
        print(f"Hiba az üzenetküldéskor: {e}")
        

# --- AI-riport funkció ---
def find_suspicious_data(temp_data_df):
    print('Vizsgálat elindult...')

    prompt = (
    "Átadok neked egy táblázat adatait JSON formátumban, amelyek ingatlan adásvételi és bérleti adatokat tartalmaznak. "
    "Egy sor mindig egy szerződéshez (ügylethez) tartozik. Egy szerződésen belül több ingatlan is szerepelhet.\n\n"

    "Kérlek, készíts egy HTML formázású riportot, amit Telegramra tudok küldeni. A riportban az alábbi szempontok szerint válogasd ki az ingatlanokat:\n\n"

    "🔍 Egy ingatlant akkor szerepeltess a riportban, ha:\n"
    "- Adásvételről van szó, és az adott szerződéshez tartozó összes ingatlan **együttes értéke meghaladja az 50 millió forintot**. \n"
    "- Haszonbérleti szerződés esetén, ha **egy ingatlan területe meghaladja az 5 hektárt**, azt akkor is add hozzá a riporthoz, ha a szerződésben több ingatlan is van.\n\n"

    "📌 A riport formátuma legyen pontosan az alábbi (csak HTML kimenetet kérek, semmi mást):\n\n"
    "<b>Figyelemre méltó ingatlanok:</b>\n\n"
    "<b>[Ingatlan ára emberileg formázva, pl. 12.3 millió Ft]</b>\n"
    "<b>[Település neve]</b>, röviden fogalmazd meg, hogy adásvétel vagy bérlet történt, mekkora a terület (hektárban), és hogy miért számít gyanúsnak\n\n"
    "<a href=\"[URL]\">Részletek</a>\n\n"
    
    "Használj üres sort a különböző ingatlanok között.\n"
    "Csak a riportot küld el, ne legyen az elején ``` html, és a végén se legyen ```"
    "Ne használj <br>, <ul>, <p> vagy más, a Telegram HTML parse_mode által nem támogatott tageket.\n\n"
    "Ha nincs semmi gyanús adat, akkor csak ezt válaszold: 'Nincs semmi gyanús adat.'"

    )
    temp_data = temp_data_df.to_dict(orient="records")
    prompt += json.dumps(temp_data, ensure_ascii=False, indent=4)

    try:
        print("Gemini API hívása...")
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        raw_response = response.text.strip()
        return raw_response

       

    except Exception as e:
        print(f"Hiba a Gemini API hívása során: {e}")
gem_resp = find_suspicious_data(df)

if (gem_resp.lower() =="nincs semmi gyanús adat."):
    print("nincs gyanus")
    pass
else:
    send_telegram_message(gem_resp, parse_mode="HTML")










