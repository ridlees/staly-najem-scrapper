import sqlite3

command = """
CREATE TABLE offers(
    Id integer primary key,
    Link MEDIUMTEXT,
    Name MEDIUMTEXT,
    Price MEDIUMTEXT,
    RentierName MEDIUMTEXT,
    RentierPhone MEDIUMTEXT,
    RentierEmail MEDIUMTEXT,
    RentierCompany MEDIUMTEXT,
    DescriptionTable LONGTEXT,
    Details LONGTEXT,
    Lat MEDIUMTEXT,
    Long MEDIUMTEXT,
    Date DATE,
    DateOfUnlisting DATE,
    UNIQUE(Link)
    
);
"""


con = sqlite3.connect("offers.db")
cur = con.cursor()
cur.execute(command)
