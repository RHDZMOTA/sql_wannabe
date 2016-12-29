# -*- coding: utf-8 -*-
"""
SQL FUNCTIONS 

@author: rhdzmota
"""

# %% 

import numpy  as np
import pandas as pd
import matplotlib.pyplot as plt
from compytibility_pack.format_string import *

# %% findKey

def findKey(_dict, find = 'min'):
    '''findKey function
    '''
    keys = list(_dict.keys())
    val  = list(_dict.values())
    if find == 'min':
        return keys[np.argmin(val)]
    if find == 'max':
        return keys[np.argmax(val)]
    
minValue = lambda _dict: findKey(_dict, 'min')
maxValue = lambda _dict: findKey(_dict, 'max')

# %% Relations

def saveRelation(arr, _path):
    '''
    '''
    np.save(_path+'relation_scheme', arr)
    
def loadRelation(_path):
    return list(np.load(_path+'relation_scheme.npy'))

# %% integerIndex

def colIndex(col, columns):
    for i in range(len(columns)):
        if col == columns[i]:
            return i
            
# index as integer
def integerIndex(df):
    pd.options.mode.chained_assignment = None
    columns = df.columns.values
    for cols in columns:
        if 'id_' in cols:
            df.loc[:,cols] = df[cols].apply(lambda x: int(x)).values
            #df[cols] = df[cols].apply(lambda x: int(x))
    pd.options.mode.chained_assignment = 'warn'
    return df 

# %% files

def filesInDir(path):
    import os
    files = []
    for file in os.listdir(path):
        if file.endswith(".csv"):
            files.append(file.split('.')[0])
    return files

# %% 

# check for fastread
def checkFastRead(path):
    import os
    everything = os.listdir(path)
    return 'fastread' in everything

# %% 

class database:
    
    desc = 'This is a database instance.'
    
    def __init__(self, reference_path = 'sql_example/' , table_names = 'all', relation_scheme = []):
        '''init
        ---- inputs
        db: list containing table names 
        relation_scheme: list containing the relations among tables as string
        '''
        self.reference_path = reference_path
        
        if table_names == 'all':
            table_names = filesInDir(reference_path)
        
        # construct database dictionary 
        db = {}
        for i in table_names:
            db[i] = reference_path+i+'.csv'
        
        self.ref = db
        
        # get relation scheme 
        if len(relation_scheme) == 0:
            self.relation_scheme = loadRelation(reference_path)
        else:
            self.relation_scheme = relation_scheme
        
        # false fast read 
        self.fast_read = False 
    
    def isFastRead(self):
        return checkFastRead(self.reference_path)
    
    def availableTables(self, silence = False, ret_rn = False):
        '''availableTables functions
        
        ---- inputs 
        
        ---- outputs
        
        '''
        table_names = []
        for i in self.ref.keys():
            table_names.append(i)
            if not silence:
                print(i)
                
        if ret_rn:
            return table_names

        
    def getCols(self, table_pd, table_name = '', colnames = []):
        '''getCols
        Filter columns 
        
        ---- inputs 
        
        ---- outputs 
        
        '''
        
        cn   = []
        for col in colnames:
            
            # check if a specif table was requested
            '''
            if '.' in col:
                col = col.split('.')
                if col[0] != table_name:
                    continue
                col = col[1]
            '''
            
            # check if column exists 
            if col in table_pd.columns:
                cn.append(col)
                
        return table_pd[cn]
            
    def readTable(self, table, colnames = []):
        '''readTable function
        Read a given table in memory. 
        
        ---- inputs
        table: string containing the table's name.
        
        ---- outputs
        A pandas dataframe with the information of the table.
        '''
        
        # TODO: verify if table exists 
        table_path = self.ref[table]
        
        if self.fast_read:
            temp_df = pd.read_pickle('{}fastread/{}.pkl'.format(self.reference_path,table))
        else:
            temp_df = pd.read_csv(table_path)
            
        temp_df = integerIndex(temp_df)
        
        if len(colnames) == 0:
            return temp_df
        return self.getCols(temp_df,table_path,colnames)
    
    def createFastRead(self):
        '''createFastRead function
        '''
        
        # create directory 
        # TODO
        
        # get available tables
        at = self.availableTables(silence = True, ret_rn = True)
        
        # save as pickle 
        for table in at: 
            self.readTable(table).to_pickle(formatString('{}fastread/{}.pkl',[self.reference_path,table]))
        
        # enable
        self.fast_read = True 

    def tablesAndCols(self, silence = False, ret_rn = False):
        '''
        '''
        table_list = self.availableTables(silence = True, ret_rn = True)
        
        ref_dict = {}
        for tab in table_list:
            a = self.readTable(tab)
            string = '\n {} :::: {}\n'
            ref_dict[tab] = list(a.columns)
            if not silence:
                print(string.format(tab, ref_dict[tab]))
            
        if ret_rn:
            return ref_dict
    
    def _where(self, table, condition):
        '''_where function
        '''
        
        # one table without condition
        if condition == '':
            return table
        
        return table.query(condition)
            
        
        
    def findRelation(self, table_name1, table_name2, silence = True):
        '''findRelation
        Not efficient. 
        '''
        rs = self.relation_scheme
        
        at = self.availableTables( silence = True, ret_rn = True)
        if (not (table_name1 in at)) or (not (table_name2 in at)):
            if not silence:
                print('Warning: table not in database.')
            #return None
        
        def findAux(val, string):
            st = string.split('--')
            for s in st:
                if val == s:
                    return True
            return False
        
        def findIn(table_name, string):
            for t in table_name.split('REF'):
                if (findAux(t, string)):
                    return True 
            return False
        
        def findInd(t1, t2, string):
            
            rel = string[string.find('--')+2:string.find('--')+4]
            p1 = -1
            p2 = -1
            
            for t in t1.split('REF'):
                if t in string:
                    p1 = string.find(t)
            
            p2 = string.find(t2)
            if p1 > p2:
                return rel[-1]+rel[0]
            return rel 
        
        save_rel = None 
        for r in rs:

            if findIn(table_name1, r) and findIn(table_name2, r):
                save_rel = r
        
        if type(save_rel) == type(None):
            if not silence:
                print('Warning: Relation not found.')
            return 0
        
        return findInd(table_name1, table_name2, save_rel)
    
    def constrTable(self, tables):
        '''
        '''
        
        # dictionary of tables and status 
        table_status = {}
        for t in tables:
            table_status[t] = 1
            
        # function to make a table from t1 -- 1n -- t2
        def makeTable(t1, t2, n1, n2):
            
            col1 = t1.columns
            col2 = t2.columns
            
            isId = lambda val: 'id_' in val
            
            def getIds(cols):
                res = []
                for c in cols:
                    if isId(c):
                        res.append(c)
                return res 
            
            idc1 = getIds(col1)
            idc2 = getIds(col2)
            
            
            def repVal(list1, list2):
                for i in list1:
                    if i in list2:
                        return i
            axis_col = repVal(idc1, idc2)
            
            def changeColName(axis_col, colnames,n):
                c = []
                for i in colnames:
                    if len(i.split('REF')) > 1:
                        #i = i.split('REF')[-1]
                        c.append(i)
                        continue
                    if not 'id' in i:
                        c.append(n+'REF'+i)
                    else:
                        c.append(i)
                return c
            
            t1.columns = changeColName(axis_col, col1, n1)
            t2.columns = changeColName(axis_col, col2, n2)
            
            t1, t2 = integerIndex(t1), integerIndex(t2)
            
            self.warning_na = False
            
            def extract(_id):
                temp = t1.query('{} == {}'.format(axis_col,_id))[c].values
                if len(temp) == 0:
                    self.warning_na = True
                    return float('nan')
                return np.asscalar(temp)            
                
            for c in t1.columns:
                if c == axis_col:
                    continue 
                    
                t2[c] = t2[axis_col].apply(extract)
                
            if self.warning_na:
                print('Warning: At least one row was dropped due to nans.')
            
            
            return integerIndex(t2.dropna())
        
        # function to check if all status in table_status are zero
        def checkStatus(table_status):
            st = 0
            for k in table_status.keys():
                st += table_status[k]
            if st == 0:
                return True
            return False 
        
        # recursive function to find match and construct final table
        def findMatch(table,table_name,table_status):

            if checkStatus(table_status):
                return table 
            
            for t in table_status.keys():
                # invalid status 
                if not table_status[t]:
                    continue
                # skip same 
                if t == table_name:
                    table_status[t] = 0
                    continue 
                # determine relation
                rel = self.findRelation(table_name, t)
                if type(rel) == type(None) or rel == 0:
                    continue
                break
            
            if type(rel) == type(None):
                return table
            
            rel = self.findRelation(table_name,t)
            new_name = table_name+'REF'+t
            
            if rel == '1n':
                table_status[t] = 0
                new_table = makeTable(table, self.readTable(t), table_name, t)
                
            if rel == 'n1':
                table_status[t] = 0
                new_table = makeTable(self.readTable(t),table,t, table_name)
                
            table_status[new_name] = 0
            
            final_table = findMatch(new_table,new_name,table_status)
            return final_table
        
        table_status[tables[0]] = 0
        temp = findMatch(self.readTable(tables[0]),tables[0],table_status)
        return temp
        
    def SELECT(self, colnames = [], _as = [], FROM = [], WHERE = '', _reset = True):
        '''SELECT function
        add desc.
        
        ---- inputs
        colnames: list containing string 
        ''' 
        
        # function to relapce dots in a string by 'REF'
        # e.g. "this is a string with a dot (.)" --> "this is a string with a dot (REF)"
        def replaceDotByREF(string):
            string = string.split('.')
            if len(string) > 1:
                tail = ''
                for i in string[1:]:
                    tail += 'REF'+i
                new_string = string[0]+tail
                return replaceDotByREF(new_string)
            return string[0]
            
        # format colnames 
        new_colnames = []
        for c in colnames:
            new_colnames.append(replaceDotByREF(c))  
        colnames = new_colnames 
        
        # format where statement
        WHERE = replaceDotByREF(WHERE)
        
            
        if len(FROM) == 1:
            return self.getCols(
                         self._where(
                             self.readTable(FROM[0], colnames = []), 
                             WHERE),colnames = colnames)
        
        if len(_as) == 0:
            _as = colnames
        else:
            new_as = []
            for i,j in zip(colnames, _as):
                if j == '':
                    new_as.append(i)
                else:
                    new_as.append(j)
                    
        tbl = self.getCols(self._where(self.constrTable(FROM), WHERE),colnames = colnames)
        tbl.columns = new_as
        
        if _reset:
            tbl.reset_index(inplace = True, drop = True)
            
        return tbl
        
    def addRows(self, table_name, values, save = False):
        '''addRows function 
        Add rows to an existing table managed by the database.
        ---- inputs
        table_name: string (name of an existing table)
        values: a dictionary containing keys as columnames and 
        '''
        
        table = self.readTable(table_name)
        
        # index
        ind = np.max(table.index.values)+1
        
        # setup newline
        line = pd.DataFrame(values, index=[ind])
        
        # modify dataframe 
        
        table = pd.concat(table, line)
        # save confition 
        if save:
            table.to_csv(formatString(self.reference_path+'{}.csv',table_name))
            if self.fast_read:
                table.to_pkl(formatString(self.reference_path+'fastread/{}.pkl', [table_name]))
        return 

        
# %% 

# %% 

# %% 

# %% 

# %% 

# %% 

# %%

# %% 

# %% 

# %% 

# %% 

# %%