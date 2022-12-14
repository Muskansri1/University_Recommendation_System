#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


import pymysql
import mysql.connector
uni_recomm = pymysql.connect(host='localhost', user='root', passwd='RootPassword123', database='unibuddydemodb')
cursor= uni_recomm.cursor()


# In[5]:


cursor.execute("CREATE TABLE University_Details (University_ID varchar(255), University_Name varchar(255))")


# In[6]:


cursor.execute("describe University_Details")
for x in cursor:
    print(x)


# In[8]:


UniversityDetails_df=pd.read_csv("University.csv")
UniversityDetails_df


# In[9]:


for i,row in UniversityDetails_df.iterrows():
    cursor.execute("INSERT INTO University_Details values (%s,%s)", (int(row['University_ID']),row['University_Name']))
uni_recomm.commit()

cursor.execute("SELECT * from University_Details")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[10]:


cursor.execute("select * from university")
for x in cursor:
    print(x)


# In[11]:


cursor.execute("ALTER Table University drop column University_Name")
for x in cursor:
    print(x)


# In[12]:


cursor.execute("select * from university")
for x in cursor:
    print(x)


# In[13]:


cursor.execute("select * from student")
for x in cursor:
    print(x)


# In[14]:


cursor.execute("ALTER Table Student drop column University_Name")
for x in cursor:
    print(x)


# In[16]:


cursor.execute("select * from recruiter")
for x in cursor:
    print(x)


# In[17]:


cursor.execute("CREATE TABLE Recruiter_Details (Recruiter_ID varchar(255), Recruiter_Name varchar(255))")


# In[19]:


RecruiterDetails_df=pd.read_csv("Final_recruiter.csv")
RecruiterDetails_df


# In[20]:


for i,row in RecruiterDetails_df.iterrows():
    cursor.execute("INSERT INTO recruiter_details values (%s,%s)", (int(row['Recruiter_ID']),row['Recruiter_Name']))
uni_recomm.commit()

cursor.execute("SELECT * from recruiter_details")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[21]:


cursor.execute("ALTER Table recruiter drop column Recruiter_Name")
for x in cursor:
    print(x)


# In[24]:


cursor.execute("select * from recruiter")
for x in cursor:
    print(x)


# In[63]:


NewProfessor_df=pd.read_csv("Professor_Final.csv")
NewProfessor_df


# In[64]:


lst_col = 'Professors' 


# In[65]:


x = NewProfessor_df.assign(**{lst_col:NewProfessor_df[lst_col].str.split(',')})


# In[66]:


x


# In[35]:


import numpy as np


# In[67]:


pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]


# In[68]:


newProfessorsList_df = pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]


# In[69]:


newProfessorsList_df


# In[70]:


newProfessorsList_df['ID']= pd.Series(range(1,newProfessorsList_df.shape[0]+1)) #starts with 1


# In[71]:


newProfessorsList_df


# In[72]:


cursor.execute("select * from professor")
for x in cursor:
    x


# In[73]:


for i,row in newProfessorsList_df.iterrows():
    cursor.execute("INSERT INTO professor values (%s,%s,%s)", (int(row['ID']),row['University_ID'],row['Professors']))
uni_recomm.commit()

cursor.execute("SELECT * from professor")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[74]:


NewResearch_df=pd.read_csv("Final_research.csv")
NewResearch_df


# In[75]:


lst_col = 'Research_Name' 


# In[76]:


x = NewResearch_df.assign(**{lst_col:NewResearch_df[lst_col].str.split(',')})
x


# In[77]:


newResearchList_df = pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]
newResearchList_df


# In[78]:


newResearchList_df['Research_ID']= pd.Series(range(1,newResearchList_df.shape[0]+1)) #starts with 1


# In[79]:


newResearchList_df


# In[81]:


cursor.execute("select * from research")


# In[82]:


cursor.execute("delete from research")


# In[83]:


for i,row in newResearchList_df.iterrows():
    cursor.execute("INSERT INTO research values (%s,%s,%s)", (int(row['Research_ID']),row['Research_Name'],row['University_ID']))
uni_recomm.commit()

cursor.execute("SELECT * from research")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[85]:


NewStudent_df=pd.read_csv("Final_student.csv")
NewStudent_df


# In[86]:


NewStudent_df = NewStudent_df.drop('University_Name', axis=1)


# In[87]:


NewStudent_df


# In[88]:


lst_col = 'Target_Major' 


# In[89]:


x = NewStudent_df.assign(**{lst_col:NewStudent_df[lst_col].str.split('/')})
x


# In[90]:


NewStudent_df = pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]
NewStudent_df


# In[93]:


NewStudent_df['student_id']= pd.Series(range(1,NewStudent_df.shape[0]+1)) #starts with 1


# In[94]:


NewStudent_df


# In[95]:


cursor.execute("delete from student")


# In[99]:


for i,row in NewStudent_df.iterrows():
    cursor.execute("INSERT INTO student values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['student_id']),row['University_ID'],row['Acceptance'],row['Program'],row['Target_Major'],row['Term'],row['Year_of_Joining'],row['GRE_Q'],row['GRE_V'],row['GRE_Total'],row['GRE_AWA'],row['TOEFL'],row['Undergrad_University'],row['Undergrad_Major'],row['CGPA'],row['CGPA_Scale'],row['Work_Ex']))
uni_recomm.commit()

cursor.execute("SELECT * from student")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[105]:


Student_df=pd.read_excel("student_data.xlsx")
Student_df


# In[106]:


Student_df = Student_df.drop('University_Name', axis=1)


# In[107]:


cursor.execute("Delete from student")


# In[109]:


for i,row in Student_df.iterrows():
    cursor.execute("INSERT INTO student values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['Student_ID']),row['University_ID'],row['Status'],row['Program'],row['Target_Major'],row['Term'],row['Year'],row['GRE_Q'],row['GRE_V'],row['GRE_Total'],row['GRE_AWA'],row['TOEFL'],row['Undergrad_University'],row['Undergrad_Major'],row['cgpa'],row['cgpaScale'],row['Work_Experience']))
uni_recomm.commit()

cursor.execute("SELECT * from student")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[119]:


cursor.execute("delete from student where Target_Major like '%/%'")


# In[120]:


cursor.execute("SELECT * from student")
records=cursor.fetchall()

print(records)
uni_recomm.commit()

