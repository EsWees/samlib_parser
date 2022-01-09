#!/usr/bin/env python3
# coding: utf-8

from requests_html import HTMLSession

from fake_useragent import UserAgent

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql.sqltypes import DateTime
from sqlalchemy import Integer, String, Column, create_engine, ForeignKey
from time import sleep

from datetime import datetime

from urllib.parse import urlparse

import re
from bs4 import BeautifulSoup

import logging

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url', nargs='?', help='This will be a url')
res = parser.parse_args()

#for key in logging.Logger.manager.loggerDict:
#    print(key)

logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)

engine = create_engine('sqlite:///database_samlib.sqlite', poolclass=StaticPool, echo=False)
Base = declarative_base()

class Comments(Base):

    __tablename__ = "Comments"

    id = Column(Integer, primary_key=True, nullable=False, unique=True, autoincrement=True)

    autor = Column(Integer, ForeignKey('Autors.id'), nullable=False)

    number = Column(Integer, unique=False)
    date = Column(DateTime(timezone=True))
    text = Column(String, nullable=False)
    source = Column(String, nullable=False)

    def __repr__(self) -> str:
        return f"{self.id=}, {self.number=}, {self.autor=}, {self.text is not None}"

    def __init__(self, date, text, number, source, autor=None) -> int:
        super(Comments, self).__init__()
        self.autor, self.date, self.text, self.number, self.source = autor, date, text, number, source
        return self.id


class Autors(Base):

    __tablename__ = "Autors"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=True, unique=False)
    link = Column(String, nullable=True, unique=False)

    def __repr__(self) -> str:
        return f"Autor.id={self.id}"

    def __init__(self, name, email="", link="") -> None:
        super().__init__()
        self.name, self.email, self.link = name, email, link

db_session = scoped_session(sessionmaker(autocommit=True, autoflush=True, bind=engine))
session = db_session()

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

requests = HTMLSession()

ua = UserAgent()

headers = {'User-Agent': ua.random}


def grabTheComments(url):

    r = requests.get(url, headers=headers).text

    number = re.findall(r"^<small>(.*)\.<\/small>.*<small><i>.*<\/i>", r, re.MULTILINE)
    data = re.findall(r"^<small>.*\.<\/small>.*<small><i>(.*)  <\/i>", r, re.MULTILINE)
    text = re.findall(r"<\/small><\/i>\n((.*\n)+?)<hr noshade>", r, re.MULTILINE)

    autors = re.findall(r'<small>([0-9]+)\.<\/small>\s(.*)\s<small><i>(.*)\s{2}<\/i>', r, re.MULTILINE)

    for i in zip(number,data,text,autors):
        autor_link = re.findall(r'href="(.*)"\s', i[3][1], re.MULTILINE)[0] if re.findall(r'href="(.*)"\s', i[3][1], re.MULTILINE) else None
        autor_email = BeautifulSoup(re.findall(r'\(<u>(.*)<\/u>\)', i[3][1], re.MULTILINE)[0], 'lxml').get_text() if re.findall(r'\(<u>(.*)<\/u>\)', i[3][1], re.MULTILINE) else None

        if re.findall(r'<b>\*?.*\>(.*)<\/a>', i[3][1], re.MULTILINE):
            autor_name = re.findall(r'<b>\*?.*\>(.*)<\/a>', i[3][1], re.MULTILINE)[0]
        elif re.findall(r'<b>(\W+)<\/b>', i[3][1], re.MULTILINE):
            autor_name = re.findall(r'<b>(\W+)<\/b>', i[3][1], re.MULTILINE)[0]
        elif re.findall(r'<b>(.*)<\/b>', i[3][1], re.MULTILINE):
            autor_name = re.findall(r'<b>(.*)<\/b>', i[3][1], re.MULTILINE)[0]
        else:
            autor_name = None

        autor = Autors(autor_name, autor_email, autor_link)

        if not session.query(Autors).filter_by(name=autor.name).first():
            session.add(autor)
        
        a = session.query(Autors).filter_by(name=autor.name).first()

        comment = Comments(datetime.strptime(i[1], '%Y/%m/%d %H:%M'), BeautifulSoup(''.join(i[2][0]), 'lxml').get_text(), i[0], url, a.id)
        if not session.query(Comments).filter_by(text=comment.text).first():
            session.add(comment)


def getLinks(url):
    #all related Links
    r = requests.get(url, headers=headers)
    urls = []
    for link in r.html.links:
        if link.startswith(urlparse(url).path) and "OPERATION" not in link and "ORDER" not in link:
            urls.append(f"{urlparse(url).scheme}://{urlparse(url).netloc}{link}")
    return urls

def getAllPages(url):
    # main pages
    pages = [f"{url}?PAGE=1"]
    for i in getLinks(url):
        if not re.findall(r".*\?PAGE=([0-9]+)$", i, re.MULTILINE):
            continue
        else:
            page = re.findall(r".*\?PAGE=([0-9]+)$", i, re.MULTILINE)[0]
        pages.append(f"{url}?PAGE={page}")

    return pages

def getAllArchivePages(url):
    # Get max archive page number
    archiveCounter = 0
    for i in getLinks(url):
        if not re.findall(r".*\.([0-9]+)$", i, re.MULTILINE):
            continue
        res = int(''.join(re.findall(r".*\.([0-9]+)$", i, re.MULTILINE)))
        if res > archiveCounter:
            archiveCounter = res
    
    # pass through archives
    masslink = []
    for i in range(1,archiveCounter + 1):
        masslink += getAllPages(f'{url}.{i}')
    
    return masslink


def writeIntoHTML(url):
    data = session.query(Comments).order_by(Comments.date).all()
    with open(f"{res.url.replace('/','_')}.html", "a+") as file:
        file.write('''
<!DOCTYPE html>
<html>
    <head>
        <title>--__--__--__--</title>
        <meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>
        <meta name='viewport' content='width=device-width, initial-scale=1' />
        <style type='text/css'>
            body { text-align: center; padding: 10%; font: 18px Helvetica, sans-serif; color: #333; }
            h1 { font-size: 50px; margin: 0; }
            article { display: block; text-align: left; max-width: 950px; margin: 0 auto; }
            a { color: #dc8100; text-decoration: none; }
            a:hover { color: #333; text-decoration: none; }
            @media only screen and (max-width : 480px) {
                h1 { font-size: 40px; }
            }
        </style>
    </head>
    <body>
    <h1>Коменты со странички ''')
        file.write(f"{url}</h1>")

    for item in data:
        autor = session.query(Autors).filter_by(id=item.autor).first()
        with open(f"{res.url.replace('/','_')}.html", "a+") as file:
            file.write(f"""
            <article>
                <div autor>
                    <h2>{item.date} <a href={autor.link}>{autor.name}</a> Email: <a href=mailto:{autor.email}>{autor.email}</a></h2>
                </div>
                <div comment>
                    <b>№{item.number}:</b></br>
                    {item.text}
                </div>
            </article>
            """)

    with open(f"{res.url.replace('/','_')}.html", "a+") as file:
        file.write(f'''
    </body>
</html>''')


def main():
    for i in getAllPages(res.url) + getAllArchivePages(res.url):
        grabTheComments(i)
        print(".", end="", flush=True)
        sleep(1)
    print("")
    writeIntoHTML(res.url)


if __name__ == "__main__":
    """
    Start the main fuction if that system running as a script
    """
    main()
