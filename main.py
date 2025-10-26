import requests as r
from bs4 import BeautifulSoup as bs
from random import randint
from time import sleep
from datetime import datetime
import re

import sqlite3

from multiprocessing import Pool, freeze_support

class offer:
    def __init__(self, link):
        self.link = link

    def setName(self, name):
        self.name = name
        
    def setPrice(self, price):
        self.price = price
        
    def setRentierName(self, rentierName):
        self.rentierName = rentierName
        
    def setRentierPhone(self, rentierPhone):
        self.rentierPhone = rentierPhone
        
    def setRentierEmail(self, rentierEmail):
        self.rentierEmail = rentierEmail

    def setDescriptionTable(self, descriptionTable):
        self.descriptionTable = descriptionTable

    def setDetails(self, details):
        self.details = details

    def setLat(self, lat):
        self.lat = lat

    def setLong(self, long):
        self.long = long

    def setDate(self, date):
        self.date = date
    

def get(url):
    sleep(randint(10,500)/1000)
    page = r.get(url)
    return page

def soup(page):
    return bs(page.content, 'html.parser')

def updateDatabase(cur, link, name, price, rentierName, rentierPhone, rentierEmail, descriptionTable, details, lat, long, date, dateofUnlisting):
    cur.execute("""INSERT OR IGNORE INTO offers (Link, Name, Price, RentierName, RentierPhone, RentierEmail, DescriptionTable, Details, Lat, Long, Date, DateOfUnlisting) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", (link, name, price, rentierName, rentierPhone, rentierEmail, descriptionTable, details, lat, long, date, dateofUnlisting, ))


def retry_listings(url, offersPage, limit = 100,):
    if offersPage.find_all("div", class_="pagination"):
        offersPage = soup(get(url + f"?limit={limit}"))
        offersPage = retry_listings(url, offersPage, limit*2)
        return offersPage
    else:
        return offersPage

def getOfferDetails(url):
    offer_ = offer(url)
    try:
        offerSoup = soup(get(url))

        #Gets name of the offer

        try:
            name = offerSoup.find_all("h1")[0].text
            offer_.setName(name)
        except:
            offer_.setName("NaN")
            
        #Gets price of the offer
            
        try:
            price = offerSoup.find_all("td", class_="price")[0].text
            offer_.setPrice(price)
        except:
            offer_.setPrice("NaN")

        try:
            rentier = offerSoup.find("div", class_="col-xs-6")
            rentierName = rentier.find("span").text
            offer_.setRentierName(rentierName)
            rentierPhone = rentier.find("span", class_="name").next_sibling.strip()
            offer_.setRentierPhone(rentierPhone)
            rentierEmail = rentier.find("a").text
            offer_.setRentierEmail(rentierEmail)
        except:
            offer_.setRentierName("NaN")
            offer_.setRentierPhone("NaN")
            offer_.setRentierEmail("NaN")

        try:
            details = offerSoup.find("div", class_="detail-nemovitosti")
            detailParagraph = details.find("p").text
            offer_.setDetails(detailParagraph)

        except:
            offer_.setDetails("NaN")

        try:
            script = None
            for s in offerSoup.find_all("script"):
                if "renderOpenStreetMap" in s.get_text():
                    script = s
                    break

            if script:
            # Extract lat and lng using regex
                match = re.search(
                r'renderOpenStreetMap\(\s*"[^"]*"\s*,\s*([0-9.-]+)\s*,\s*([0-9.-]+)',
                script.get_text()
            )
                if match:
                    lat, lng = match.groups()
                    offer_.setLat(lat)
                    offer_.setLong(lng)
                else:
                    offer_.setLat("NaN")
                    offer_.setLong("NaN")
            else:
                offer_.setLat("NaN")
                offer_.setLong("NaN")

        except Exception as e:
            print("Error extracting lat/lng:", e)
            offer_.setLat("NaN")
            offer_.setLong("NaN")

        try:
            pairs = []
            try:
                for row in offerSoup.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) == 2:
                        key = cols[0].get_text(strip=True) if cols[0] else ""
                        value = cols[1].get_text(strip=True) if cols[1] else ""
                        if key and value:
                            pairs.append(f"{key}: {value}")
            except Exception as e:
                print("Error while parsing table:", e)

            description = " | ".join(pairs)
            offer_.setDescriptionTable(description)

        except:
            offer_.setDescriptionTable("NaN")


        today = datetime.now().date()
        offer_.setDate(str(today))
        
        return offer_
    
    except Exception as e:
        print("Error in getting offer details", e)

def processOffer(houseOffer, dateOfUnlisting):
    con = sqlite3.connect("offers.db")
    cur = con.cursor()
    ahref = houseOffer.find_all("a")[0].get("href")
    if not cur.execute("SELECT EXISTS(SELECT 1 FROM offers WHERE Link=?);", (ahref,)).fetchone()[0]:
        offer_ = getOfferDetails(ahref)
        updateDatabase(cur, offer_.link, offer_.name, offer_.price, offer_.rentierName, offer_.rentierPhone, offer_.rentierEmail, offer_.descriptionTable, offer_.details, offer_.lat, offer_.long, offer_.date, dateOfUnlisting)
        con.commit()
    else:
        cur.execute(f"UPDATE offers SET DateOfUnlisting = ? WHERE Link = ?;", (dateOfUnlisting,ahref,))
        con.commit()

    return f"Page {ahref} is complete"



def main():
    url = f"https://nemovitosti.stalynajem.cz/"
    
    con = sqlite3.connect("offers.db")
    updateDelist = con.cursor()
    
    offersPage = soup(get(url))
    offersPage = retry_listings(url, offersPage, 20)
    listings = offersPage.find_all("div", class_="box container")
    print(len(listings))

    dateOfUnlisting = str(datetime.fromtimestamp(0).date())
    updateDelist.execute("UPDATE offers SET DateOfUnlisting = 0 WHERE DateOfUnlisting = ?;",(dateOfUnlisting,))
    con.commit()


    freeze_support()
    with Pool(processes=10) as pool:
        results = [pool.apply_async(processOffer, args=(houseOffer, dateOfUnlisting)) for houseOffer in listings]
        final_results = [r.get() for r in results]
        print(final_results)
    today = str(datetime.now().date())
    updateDelist.execute("UPDATE offers SET DateOfUnlisting = ? WHERE DateOfUnlisting = ?;", (today,0))
    con.commit()

if __name__ == '__main__':
    main() 
