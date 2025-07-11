from dotenv import load_dotenv
import pandas as pd
import requests
import os
from tqdm import tqdm
import json
import pickle
from openai import OpenAI
from datetime import datetime
import time
from telegram.helpers import escape_markdown
import telebot

load_dotenv()

# summarize with gemini
OPEN_AI_API_KEY = os.environ.get('GEMINI')
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')  # foldek chat ID

bot = telebot.TeleBot(BOT_TOKEN)

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
new_data['id'] = new_data['id'].astype(str)
old_df = pd.read_pickle('all_data.pickle')
old_df['azonosító'] = old_df['azonosító'].astype(str)


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
    time.sleep(5)  # To avoid hitting the API too fast
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
        "  \"vételárak összegzése\": \"ár_ide az összes ár összeadva számként amenyiben adásvétel történt\",\n"
        "  \"bérleti díj\": \"ár_ide az összes ár összeadva számként amenyiben haszonbérlet történt\",\n"
        "  \"pénznem\": \"HUF vagy EUR\",\n"
        "}\n\n"
        "JSON:\n" 
    )
    prompt += json.dumps(temp_data, ensure_ascii=False, indent=4) + "\n"

    try:
        print("OPENAI API hívása...")
        client = OpenAI(
            #api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
            api_key = OPEN_AI_API_KEY
        )

        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="gpt-4o-mini",
        )


        raw_json_string = chat_completion.choices[0].message.content.strip()

        # remove``` jsoon from the beginining and end if present
        # if raw_json_string.startswith('```json'):
        # Tisztítás: ha ```json ... ``` blokkban jött vissza
        
        if raw_json_string.startswith('```json'):
            raw_json_string = raw_json_string[8:].strip()
        if raw_json_string.endswith('```'):
            raw_json_string = raw_json_string[:-3].strip()

        # Csak az első `[` és az utolsó `]` közötti rész megtartása
        start_idx = raw_json_string.find('[')
        end_idx = raw_json_string.rfind(']')
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            raw_json_string = raw_json_string[start_idx:end_idx + 1]

        data = json.loads(raw_json_string)



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
        print(f"Hiba a openai API hívása során: {e}")


# with 20 line batching get the df and combine them
batch_size = 20
all_dfs = []
for i in tqdm(range(0, len(all_data_json), batch_size)):
    batch = all_data_json[i:i + batch_size]
    df = get_batch_info_df(batch)
    if df is not None:
        all_dfs.append(df)

 

# Combine all DataFrames into one
if all_dfs:
    final_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Összesített DataFrame mérete: {final_df.shape}")
    # Save the combined df 
    final_df_to_save = pd.concat([old_df, final_df], axis=0, ignore_index=True)

    print(f'i save new data {len(final_df_to_save)}')

    final_df_to_save.to_pickle('all_data.pickle')



### REPORT IT

big_prop = final_df[final_df['vételárak összegzése']>50_000_000]
if len(big_prop)>0:
    
    report_lines = ['Figyelemre méltó ingatlanok:\n\n']
    for _, row in big_prop.iterrows():
        price_millions = row["vételárak összegzése"] / 1_000_000
        report_lines.append(f"<b>Ingatlan ára: {price_millions:.1f} millió Ft</b>")
        report_lines.append(f"<b>{row['település']}</b>, helyrajzi számok száma: {row['helyrajzi számok száma']}	")
        report_lines.append(f"<a href=\"{row['Link a részletekhez']}\">Részletek</a>")
        report_lines.append("\n")

    # Eredmény kiírása
    html_report = "\n".join(report_lines)
    print(f'ez lenne a z üzi{html_report}')
    #bot.send_message(chat_id=CHAT_ID, text=html_report, parse_mode="HTML")
