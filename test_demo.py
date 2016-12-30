# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27 23:41:54 2016

@author: rhdzm
"""

from sql_functions import *

# %% Create database instance

db = database(reference_path  = 'sql_example/')
db.fast_read = True 

# %% Enable readfast 

db.createFastRead()

# %% Availabe tables 

string = '\nAvailable tables:\n\n>> {}\n\n Relation Scheme:\n\n>> {}'

print(formatString(string,
                   [db.availableTables(silence=True,ret_rn=True),
                    db.relation_scheme]))

# %% Read table 

db.readTable('Users')

# %% Perform analytics 
import matplotlib.pyplot as plt 

db.readTable('Users').sex.value_counts().plot(kind = 'bar')
plt.title("Bar Plot: Frequency of user's sex")
plt.ylabel('Observations')
plt.grid()
plt.show()

db.readTable('Users').weight.plot(kind = 'kde')
plt.title('Density distribution for weight variable')
plt.xlabel('weight in kg')
plt.grid()


# %% Relation amog tables 

table_test1, table_test2 = 'Role', 'UserTeam'

string = 'The relation between "{}" and "{}" is {}.'
print(formatString(string, [table_test1, table_test2, 
                            db.findRelation(table_test1, table_test2)]))


# %% Join tables 

db.constrTable(['Teams', 'Activities'])

# %%

db.constrTable(['UserTeam','Users','Teams','Role'])

# %% Query 

db.SELECT(colnames = ['Users.name', 'Teams.color'], 
          _as      = ["User's Name","Team color"],
          
          FROM     = ['UserTeam','Users','Teams'], 
          WHERE    = ''
          
         )


# %% QUERY 

db.SELECT(colnames = ['Users.name','Role.description','Teams.color','Activities.description'], 
          _as      = ['Name','Role','Team color','Act'],
          
          FROM     = ['UserTeam','Users','Teams', 'Role','Activities'], 
          WHERE    = 'Teams.color == "green"'
          
         )

# %% ADD NEW ROWS

db.colsFrom('Users')

line_values = {
'id_user':8,
'name':'Alejandra',
'sex':'Female',
'age':22,
'weight':50,
'height':1.70,
'nationality':'mexican',
'ethnic':'latin'
}

print('\nNew table:')
db.addRow('Users',line_values)

# %% 

# %% 