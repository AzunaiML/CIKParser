# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 00:22:58 2016

@author: Azunai
"""

from bs4 import BeautifulSoup
import urllib
import pandas as pd
import time
import numpy as np
from multiprocessing import Pool

uiks_file_path = 'uik.csv'
result_file_path = 'result.csv'
n_proc = 8
columns_list = ['uik',
               'voters',
               'bulletin',
               'bulletin_before_ellections',
               'bulletin_on_ellections_in_uik',
               'bulletin_on_ellections_outside_uik',
               'ended_bulletin',
               'bulletin_in_portable_box',
               'bulletin_in_stationary_box',
               'invalid_bulletin',
               'not_invalid_bulletin',
               'got_absentee_ballot',
               'given_absentee_ballot',
               'voters_by_absentee_on_uik',
               'ended_absentee_ballot',
               'given_absentee_ballot_by_tik',
               'lost_absentee_ballot',
               'lost_bulletin',
               'not_counted_bulletin',
               'votes_RODINA',
               'votes_KPKR',
               'votes_RPPS',
               'votes_ER',
               'votes_REP_GREEN',
               'votes_GP',
               'votes_LDPR',
               'votes_PARNAS',
               'votes_PR',
               'votes_GS',
               'votes_JABLOKO',
               'votes_KPRF',
               'votes_PATRIOTS',
               'votes_SR']

def parser(uik):
    uik = uik[1]
    if 'kaliningrad' not in uik['url']:
        url = uik['url'].replace('&type=0','&type=242')
        row = []
        page = ''
        row.append(uik['tik'])
        row.append(uik['uik'])
        i = 0
        while page == '' and i < 10:
            try:
                page = urllib.urlopen(url).read()
                i += 1
            except:
                time.sleep(5)
                i += 1
        if page != '':
            page = page.decode('windows-1251')
            soup = BeautifulSoup(page, from_encoding='windows-1251')
            table = soup.find('table', attrs={'bgcolor': '#ffffff'})
            tr = table.find_all('tr')
            for j in xrange(len(tr)):
                td = tr[j].find_all('td')
                if len(td)>2:
                    if j<18:
                        row.append(int(td[2].get_text()))
                    else:
                        row.append(int(str(td[2].contents[0]).replace('<b>','').replace('</b>','')))
        if len(row)<33:
            for k in xrange(33-len(row)):
                row.append(np.NaN)
        return row

if __name__ == '__main__':
    uiks = pd.read_csv(uiks_file_path, 
                       delimiter = ';')
    rows  = []
    pool = Pool(processes=n_proc)
    result_list = pool.map(parser, (uik for uik in uiks.iterrows()))
    df = pd.DataFrame(data=result_list, columns=columns_list)
    df.to_csv(result_file_path, delimiter=';', encoding='utf-8')
    
