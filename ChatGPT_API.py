import pandas as pd
import numpy as np
from airtable import airtable
from pyairtable import Table
import openai
import re
import time
import warnings
import datetime
import os
import json
warnings.filterwarnings('ignore')

def get_item_list(x):
    try:
        return x[0]
    except:
        return None

def get_pub_year(df_view_clean):
    n = df_view_clean.shape[0]
    for u in df_view_clean.index:
        try:
            year = int(df_view_clean.loc[u, 'Publ Year'])
            if not np.isnan(year):
                df_view_clean.loc[u, 'Pub_yr'] = int(year)
                print(f"Skipping Record {u+1}/{n} ...")
                continue
        except:
            pass
        for _ in range(5):
            try:
                completion = openai.ChatCompletion.create(
                api_key='Type_Your_Key',
                model="gpt-3.5-turbo",
                messages=[
                {"role": "user", "content": "What is the original/earliest publication year of this text? " + str(df_view_clean.loc[u, 'Combination']) + " Provide a simple response with just the year with no period at the end."}], request_timeout=5)
                df_view_clean.loc[u, 'Pub_yr'] = completion.choices[0].message.content
                print(f"Record {u+1}/{n} is processed successfully!")
                break
            except Exception as err:
                time.sleep(10)
                print('Waring: ChatGPT API reported the below error, retrying ...')
                print(str(err))

    return df_view_clean

def get_format(df_view_clean):
    n = df_view_clean.shape[0]
    for u in df_view_clean.index:
        try:
            form = df_view_clean.loc[u, 'Format']
            if not (isinstance(form, (float, np.floating)) or (isinstance(form, str) and form.lower() == "nan")):
                print(f"Skipping Record {u+1}/{n} ...")
                continue
        except:
            pass
        for _ in range(5):
            try:
                completion = openai.ChatCompletion.create(
                api_key='Type_Your_Key',
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": "Assign a format to the text of " + str(df_view_clean.loc[u, 'Combination']) + " using exclusively the following list:  Anthology/Varied Collection, Autobiography/Memoir, Biography, Book, Coffee Table Book, Collection of Letters, Cookbook, Essay Collection, Essay/Speech, Graphic Memoir, Graphic Novel/Book, Memoir in Verse, Novel, Novel/Book in Verse, Novella, Picture Book, Play, Poem, Reference/Text Book, Screenplay/Script, Scripture, Short Story, Short Story Collection. Do not include a period."}], request_timeout=5)
                df_view_clean.loc[u, 'Format'] = completion.choices[0].message.content
                print(f"Record {u+1}/{n} is processed successfully!")
                break
            except Exception as err:
                time.sleep(10)
                print('Waring: ChatGPT API reported the below error, retrying ...')
                print(str(err))

    return df_view_clean  

def get_audience(df_view_clean):
    n = df_view_clean.shape[0]
    for u in df_view_clean.index:
        try:
            audience = df_view_clean.loc[u, 'Audience']
            if not (isinstance(audience, (float, np.floating)) or (isinstance(audience, str) and audience.lower() == "nan")):
                print(f"Skipping Record {u+1}/{n} ...")
                continue
        except:
            pass
        for _ in range(5):
            try: 
                completion = openai.ChatCompletion.create(
                api_key='Type_Your_Key',
                model="gpt-3.5-turbo",
                messages=[
                {"role": "user", "content": "Assign an audience to the text of " + str(df_view_clean.loc[u, 'Combination']) + " using the following list: Adult, Middle Grade, Young Adult. Do not include a period."}], request_timeout=5)
                df_view_clean.loc[u, 'Audience'] = completion.choices[0].message.content
                print(f"Record {u+1}/{n} is processed successfully!")
                break
            except Exception as err:
                time.sleep(10)
                print('Waring: ChatGPT API reported the below error, retrying ...')
                print(str(err))

    return df_view_clean

def get_FnF(df_view_clean):
    n = df_view_clean.shape[0]
    for u in df_view_clean.index:
        try:
            fiction = df_view_clean.loc[u, 'F/NF']
            if not (isinstance(fiction, (float, np.floating)) or (isinstance(fiction, str) and fiction.lower() == "nan")):
                print(f"Skipping Record {u+1}/{n} ...")
                continue
        except:
            pass
        for _ in range(5):
            try:
                completion = openai.ChatCompletion.create(
                api_key='Type_Your_Key',
                model="gpt-3.5-turbo",
                messages=[
                {"role": "user", "content": "Assign a category to the text of " + str(df_view_clean.loc[u, 'Combination']) + " using exclusively the following list: Fiction, Nonfiction. Categorize poems as Fiction. Do not include the word poem or poetry in the result; do not include a period."}], request_timeout=5)
                df_view_clean.loc[u, 'F/NF'] = completion.choices[0].message.content
                print(f"Record {u+1}/{n} is processed successfully!")
                break
            except Exception as err:
                time.sleep(10)
                print('Waring: ChatGPT API reported the below error, retrying ...')
                print(str(err))

    return df_view_clean

def get_item(x):
    try:
        return x[0]
    except:
        return None

def try_int(x):
    try:
        x =  int(x)
        if x < 100 or x > datetime.date.today().year:
            return None
        return x
    except:
        return None

def try_convert(x):
    try:
        if np.isnan(x):
            return None
        else:
            return int(x)
    except:
        return None

def try_convert_str(x):
    try:
        x = str(x)
    except:
        x = None
        
    if x == 'nan':
        x = None
    return x

def query_ChatGPT():

    print('-'*75)
    print('Processing input data ...')
    # reading the titles from Excel
    df_view_clean = pd.read_excel('titles.xlsx')
    df_view_clean[['Author', 'Title']] = df_view_clean[['Author', 'Title']].astype(str)
    df_view_clean['Author'] = df_view_clean['Author'].apply(lambda x: x.replace('"', '').strip())
    df_view_clean['Title'] = df_view_clean['Title'].str.split('By ').map(lambda x: get_item_list(x))
    df_view_clean['Combination'] = df_view_clean['Title'] + ' by ' + df_view_clean['Author']

    df_view_clean.to_excel('Query_Results.xlsx', index=False)
    print('-'*75)
    print('Querying ChatGPT for literature Fiction ...')
    df_view_clean = get_FnF(df_view_clean)
    print('Exporting to Excel ...')
    df_view_clean.to_excel('Query_Results.xlsx', index=False)

    print('-'*75)
    print('Querying ChatGPT for literature Audience ...')
    df_view_clean = get_audience(df_view_clean)
    print('Exporting to Excel ...')
    df_view_clean.to_excel('Query_Results.xlsx', index=False)

    print('-'*75)
    print('Querying ChatGPT for Format data ...')
    df_view_clean = get_format(df_view_clean)
    print('Exporting to Excel ...')
    df_view_clean.to_excel('Query_Results.xlsx', index=False)

    print('-'*75)
    print('Querying ChatGPT for literature Publication Date ...')
    df_view_clean = get_pub_year(df_view_clean)
    print('Exporting to Excel ...')
    df_view_clean.to_excel('Query_Results.xlsx', index=False)

    # Cleaning and Auditing
    print('-'*75)
    print('Auditing literature Fiction ...')
    ## Fiction / NonFiction
    df_view_clean['F/NF'] = df_view_clean['F/NF'].str.replace('.', '')
    df_view_clean['F/NF'] = df_view_clean['F/NF'].str.replace('Fiction Fiction Fiction Fiction', 'Fiction')
    inds = df_view_clean[df_view_clean['F/NF'].isin(['Fiction', 'Nonfiction']) == False].index
    n = len(inds)
    for u in inds:
        edited = False
        for elem in ["Fiction", "Nonfiction"]:
            if elem.lower() in df_view_clean.Audience.loc[u].lower():
                df_view_clean.loc[u, 'F/NF'] = elem
                edited = True
                break
        if edited: continue
        for _ in range(5):
            try:
                completion = openai.ChatCompletion.create(
                api_key='Type_Your_Key',
                model="gpt-3.5-turbo",
                messages=[
                {"role": "user", "content": "Assign a category to the text of " + str(df_view_clean.loc[u, 'Combination']) + " using the following list: Fiction, Nonfiction. Categorize poems as Fiction. Do not include the word poem or poetry in the result; do not include a period."}], request_timeout=5)
                df_view_clean.loc[u, 'F/NF'] = completion.choices[0].message.content
                print(f"Record {u+1}/{n} is processed successfully!")
                break
            except Exception as err:
                time.sleep(10)
                print('Waring: ChatGPT API reported the below error, retrying ...')
                print(str(err))

    ## Publication Year
    print('-'*75)
    print('Auditing literature Publication Date ...')
    df_view_clean['Pub_yr'] = df_view_clean['Pub_yr'].astype(str)
    df_view_clean['Publ Year'] = df_view_clean['Pub_yr'].map(lambda x: get_item(re.findall(r'\d+', x)))
    df_view_clean['Publ Year'] = df_view_clean['Publ Year'].map(lambda x: try_int(x))

    ## Audience
    print('-'*75)
    print('Auditing literature Audience ...')
    inds = df_view_clean.index
    n = len(inds)
    for u in inds:
        try:
            df_view_clean.loc[u, 'Audience'] = df_view_clean.loc[u, 'Audience'].strip('.')
        except:
            pass

        if df_view_clean.Audience.loc[u] not in ["Adult", "Middle Grade", "Young Adult", 'YA', 'MG']:
            edited = False
            for elem in ["Adult", "Middle Grade", "Young Adult", 'YA', 'MG']:
                if elem.lower() in str(df_view_clean.Audience.loc[u]).lower():
                    df_view_clean.loc[u, 'Audience'] = elem
                    edited = True
                    break
            if edited: continue
            for _ in range(5):
                try:
                    completion = openai.ChatCompletion.create(
                    api_key='Type_Your_Key',
                    model="gpt-3.5-turbo",
                    messages=[
                    {"role": "user", "content": "Assign the Category that best fits " + str(df_view_clean.loc[u, 'Audience']) + " using exclusively one item of the following list: Adult, Middle Grade, Young Adult. Do not include a period."}], request_timeout=5)
                    df_view_clean.loc[u, 'Audience'] = None
                    for elem in ["Adult", "Middle Grade", "Young Adult", 'YA', 'MG']:
                        if elem.lower() in str(completion.choices[0].message.content).lower():
                            df_view_clean.loc[u, 'Audience'] = elem
                    print(f"Record {u+1}/{n} is processed successfully!")
                    break
                except Exception as err:
                    time.sleep(10)
                    print('Waring: ChatGPT API reported the below error, retrying ...')
                    print(str(err))

    df_view_clean.Audience = df_view_clean.Audience.str.replace('Young Adult', 'YA').str.replace('Middle Grade', 'MG')
    df_view_clean[df_view_clean.Audience.isin(["Adult", "Middle Grade", "Young Adult"])== False]

    ## Format
    print('-'*75)
    print('Auditing literature Format ...')
    formats = ["Anthology/Varied Collection", "Autobiography/Memoir", "Biography", "Book", "Coffee Table Book", "Collection of Letters", "Cookbook", "Essay Collection", "Essay/Speech", "Graphic Memoir", "Graphic Novel/Book", "Memoir in Verse", "Novel", "Novel/Book in Verse", "Novella", "Picture Book", "Play", "Poem", "Reference/Text Book", "Screenplay/Script", "Scripture", "Short Story", "Short Story Collection"]
    inds = df_view_clean.index
    n = len(inds)
    for u in inds:
        if df_view_clean.Format.loc[u] not in formats:
            edited = False
            for elem in formats:
                if elem.lower() in str(df_view_clean.Format.loc[u]).lower():
                    df_view_clean.loc[u, 'Format'] = elem
                    edited = True
                    break
            if edited: continue
            for _ in range(5):
                try:
                    completion = openai.ChatCompletion.create(
                    api_key='Type_Your_Key',
                    model="gpt-3.5-turbo",
                    messages=[
                    {"role": "user", "content": "Assign the Category that best fits " + str(df_view_clean.loc[u, 'Format']) + " using exclusively the following list: Anthology/Varied Collection, Autobiography/Memoir, Biography, Book, Coffee Table Book, Collection of Letters, Cookbook, Essay Collection, Essay/Speech, Graphic Memoir, Graphic Novel/Book, Memoir in Verse, Novel, Novel/Book in Verse, Novella, Picture Book, Play, Poem, Reference/Text Book, Screenplay/Script, Scripture, Short Story, Short Story Collection. Do not include a period."}], request_timeout=5)
                    
                    df_view_clean.loc[u, 'Format'] = None
                    for elem in formats:
                        if elem.lower() in str(completion.choices[0].message.content).lower():
                            df_view_clean.loc[u, 'Format'] = elem

                    print(f"Record {u+1}/{n} is processed successfully!")
                    break
                except Exception as err:
                    time.sleep(10)
                    print('Waring: ChatGPT API reported the below error, retrying ...')
                    print(str(err))

    df_view_clean.Format = df_view_clean.Format.str.replace('Novel/Book Series', 'Novel')
    df_view_clean[df_view_clean.Format.isin(["Anthology/Varied Collection", "Autobiography/Memoir", "Biography", "Book", "Coffee Table Book", "Collection of Letters", "Cookbook", "Essay Collection", "Essay/Speech", "Graphic Memoir", "Graphic Novel/Book", "Memoir in Verse", "Novel", "Novel/Book in Verse", "Novella", "Picture Book", "Play", "Poem", "Reference/Text Book", "Screenplay/Script", "Scripture", "Short Story", "Short Story Collection"]) == False]

    # exporting to Excel for further investigation
    print('-'*75)
    print('Exporting to Excel ...')
    df_view_clean.to_excel('Query_Results.xlsx', index=False)

    return df_view_clean


if __name__ == "__main__":

    start_time = time.time()
    df_view_clean = pd.DataFrame()

    try:
         df_view_clean = query_ChatGPT()
    except Exception as err:
         print('The below error occurred while querying ChatGPT')
         print(str(err))

    mins = round((time.time() - start_time)/60, 2)
    hrs = round(mins / 60, 2)
    print('-'*75)
    print(f'Process completed. Elsapsed time {hrs} hours ({mins} mins)')
    #input('Press any key to exit.')