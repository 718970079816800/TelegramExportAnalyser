#Analyse Telegram Export Folder(TEF)
#Store time, group_name, name, message, and hash in mysql database.
#Discover Telegram group links in message, and store links in mysql databases.

#Mysql database name:'tg_dialogues'
#Mysql table name:'dialogues'
#Table column:'time':datetime(6)
#Table column:'group_name':varchar(50)
#Table column:'name':varchar(50)
#Table column:'message':varchar(9999)
#Table column:'hash':varchar(32) #make an unique index on this column

#Mysql database name:'tg_dialogues'
#Mysql table name:'groups'
#Table column:'link':varchar(99) #make an unique index on this cloumn
#Table column:'title':varchar(999)
#Table column:'members':varchar(99)
#Table column:'description':varchar(999)
#Table column:'joined':BOOLEAN

import codecs, re, os, hashlib, time
import requests
import pymysql
from bs4 import BeautifulSoup
from datetime import datetime as dt

TEF = r'E:\TG_Export_202002-202003'

class TGGroup(object):
    
    link = ''
    title = ''
    page_extra = ''
    page_description = ''
    tof = True

    def __init__(self, link):
        self.link = link

    def verify(self):
        t = re.compile('^Telegram: ')
        try:
            r = requests.get(self.link)
            soup = BeautifulSoup(r.text,'lxml')
            try:
                self.title = soup.find('meta', attrs={'property':'og:title'}).attrs['content']
            except AttributeError:
                self.title = 'None'
            finally:
                try:
                    self.page_extra = soup.find('div', attrs={'class':'tgme_page_extra'}).string
                except AttributeError:
                    self.page_extra = 'None'
                finally:
                    try:
                        self.page_description = soup.find('div', attrs={'class':'tgme_page_description', 'dir':'auto'}).get_text()
                    except AttributeError:
                        self.page_description = 'None'
            if t.match(self.title):
                self.tof = False
        except Exception as e:
            print(e)

def list_html_files(rootdir):
#Return all html files in a specified dir including sub dir.
    _files = []
    _list = os.listdir(rootdir)
    for i in range(0,len(_list)):
           path = os.path.join(rootdir,_list[i])
           if os.path.isdir(path):
               _files.extend(list_html_files(path))
           if os.path.isfile(path):
               if os.path.splitext(path)[1] == '.html':
                   _files.append(path)
    return _files

def filter_html_files(rootdir):
#Document the formatted html files.
#Only process the unformatted html files.
    try:
        tobeformatted_list = []
        with codecs.open(r'D:\TG_Export\htmls_formatted_hash','r',encoding='utf-8') as f:
            formatted_hash_list = f.readlines()
            for html in list_html_files(rootdir):
                g = open(html,'rb')
                unformatted_hash = hashlib.md5(g.read()).hexdigest()
                for formatted_hash in formatted_hash_list:
                    if unformatted_hash == formatted_hash.strip():
                        pass
                    else:
                        tobeformatted_list.append(html)
                        #return html
            for tobeformatted in tobeformatted_list:
                with codecs.open('htmls_tobeformatted_hash','a',encoding='utf-8') as a:
                    a.write(tobeformatted+'\n')
    except IOError:
        with codecs.open(r'D:\TG_Export\htmls_formatted_hash','a',encoding='utf-8') as f:
            for html in list_html_files(rootdir):
                g = open(html,'rb')
                unformatted = hashlib.md5(g.read()).hexdigest()
                f.write(unformatted+'\n')
                return html

def extract_grouplink(text):
    l = []
    link1 = re.compile('https://t.me/[A-Za-z0-9_]+')
    link2 = re.compile('https://t.me/joinchat/[A-Za-z0-9_-]{22}')
    if link1.findall(text):
        for i in range(len(link1.findall(text))):
            l.append(link1.findall(text)[i])
    if link2.findall(text):
        for i in range(len(link2.findall(text))):
            l.append(link2.findall(text)[i])
    l = list(set(l))
    return l

def extract_diaglog(rootdir):
    imp = 0
    dup = 0
    err = 0
    grp = []
    for htmlpage in list_html_files(rootdir):
        try:
            soup = BeautifulSoup(open(htmlpage,encoding='utf8'),'lxml')
            head = soup.find('div', class_="page_header")
            try:
                grname = head.find('div',class_='text bold').text.strip()
            except AttributeError:
                grname = 'None'
            msgs = soup.find_all(class_=lambda x: x and x.startswith('message default'))
            for msg in msgs:
                _time = msg.find('div', class_="pull_right date details").get('title')
                date = dt.strptime(_time.split()[0], '%d.%m.%Y').strftime('%Y-%m-%d')
                hr = dt.strptime(_time.split()[1], '%H:%M:%S').strftime('%H:%M:%S')
                _time = date + ' ' + hr
                try:
                    name = msg.find('div', class_="from_name").text.strip()
                except AttributeError:
                    name = 'None'
                try:
                    text = msg.find('div', class_="text").text.strip()
                except AttributeError:
                    text = 'None'

                _str = ('Time:'+date+' '+hr+'\n'+
                        'GroupName:'+grname+'\n'+
                        'Name:'+name+'\n'+
                        'Message:'+text+'\n')   
                _hash = hashlib.md5(_str.encode('utf8')).hexdigest()

                if len(extract_grouplink(text)) == 0:
                    pass
                else:
                    for i in extract_grouplink(text):
                        grp.append(i)

                sql = ("INSERT \
                        INTO \
                        dialogues(time, \
                                group_name, \
                                name, \
                                message, \
                                hash) \
                                VALUES ('%s', '%s',  '%s',  '%s',  '%s')" 
                                % (_time, \
                                pymysql.escape_string(grname), \
                                pymysql.escape_string(name), \
                                pymysql.escape_string(text), \
                                _hash))
                try:
                    db = pymysql.connect(
                        host="127.0.0.1",
                        port=3306,
                        user="root",
                        passwd="",
                        db="tg_dialogues",
                        charset='utf8mb4'
                        )
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    db.close()
                    imp += 1
                except pymysql.err.IntegrityError as e:
                    dup += 1
                    print('Duplicate:', e)
                    db.close()
                except pymysql.err.OperationalError as e:
                    err += 1
                    print('Error:', e)
                    pass
        except FileNotFoundError as e:
            print(e)
            time.sleep(10)
            pass
        except UnicodeDecodeError as e:
            print(e)
            time.sleep(10)
            pass
    print('Imported count: ' + str(imp))
    print('Duplicated count: ' + str(dup))
    print('Warning count: ' + str(err))
    grp = list(set(grp))
    return grp

def import_grplinks(text):
    a = TGGroup(text.strip())
    a.verify()
    if a.tof:
        sql = ("INSERT INTO \
                grouplinks\
                (link, \
                 title, \
                 members, \
                 description, \
                 joined) VALUES \
                 ('%s', '%s', '%s', '%s', '%i')" % \
                (pymysql.escape_string(text), \
                 pymysql.escape_string(a.title), \
                 pymysql.escape_string(a.page_extra), \
                 pymysql.escape_string(a.page_description), \
                 0))
        try:
            db = pymysql.connect(host="127.0.0.1", \
                                 port=3306, \
                                 user="root", \
                                 passwd="", \
                                 db="tg_dialogues", \
                                 charset='utf8mb4')
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            db.close()
        except Exception as e:
            print(e)
        

if __name__ == '__main__':
    grplinks = extract_diaglog(TEF)
    for link in grplinks:
        import_grplinks(link)
