import sqlite3
import csv
import re

def split_name(name):    
    parts = [p.strip() for p in name.split(",")]
    city = None
    if len(parts) >= 3:
        candidate = parts[2]

        if re.search(r'\d+\s*m', candidate, re.IGNORECASE):
            if len(parts) >= 4:
                candidate = parts[3]
        if re.search(r'(?i)\b[1-9]\d*\s*[+＋]\s*kk\b', candidate, re.IGNORECASE):
            if len(parts) >= 5:
                candidate = parts[4]

        if candidate.lower().startswith("ul") and len(parts) >= 5:
            candidate = parts[4]

        city = candidate

    street_match = re.search(r'\bul\.[^,]+', name)
    street = street_match.group(0).strip() if street_match else "NaN"

    return city, street

def enhanceData(data):
    processed_rows = []
    for row in data:
        name = row[2]
        lat = row[7]
        long = row[8]
    
        new_row = list(row)

        gps = str(lat  + " " + long)

        "Podnájem Podnájem, Byty 2+kk, 40m², Praha 10 - Záběhlice, ulice Jetelová, Ev.č.: 03057"
        city, street = split_name(name)
        
        if street.lower().startswith(" ul"):
            temp = city
            city = street
            street = temp
            
        new_row.insert(7, city)
        new_row.insert(8, street)
        new_row.insert(9,gps)
        processed_rows.append(new_row)
        
    return processed_rows

def save2CSV(data):
    with open('data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Id', 'Link', 'Name', 'Price', 'Rentier-Name', "RentierPhone","RentierEmail","City", "Street", "Gps", "Lat", 'Long', 'Date', 'DateOfUnlisting'])
        data = enhanceData(data)
        writer.writerows(data)


con = sqlite3.connect("offers.db")
cur = con.cursor()
save2CSV(cur.execute("SELECT Id, Link, Name, Price, RentierName, RentierPhone, RentierEmail, Lat, Long, Date, DateOfUnlisting FROM offers"))    
