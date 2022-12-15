#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[5]:


import pymysql
import mysql.connector
uni_recomm = pymysql.connect(host='localhost', user='root', passwd='RootPassword123', database='unniversity_recommendation_system')
cursor= uni_recomm.cursor()


# In[ ]:


#INSERTING VALUES INTO THE TABLES


# In[13]:


University_df=pd.read_csv("Final_university.csv",encoding= 'unicode_escape')
University_df


# In[11]:


cursor.execute("alter table university add column Cost_of_Living varchar(255)")


# In[14]:


for i,row in University_df.iterrows():
    cursor.execute("INSERT INTO university values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['University_ID']),row['avg_pay_scale'],row['Duration'],row['Term'],row['Year_of_Joining'],row['Major'],row['Avg_Fees'],row['Location'],row['Cost_of_Living']))
uni_recomm.commit()

cursor.execute("SELECT * from university")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[15]:


UniversityDetails_df=pd.read_csv("UniversityDetails.csv",encoding= 'unicode_escape')
UniversityDetails_df


# In[16]:


for i,row in UniversityDetails_df.iterrows():
    cursor.execute("INSERT INTO university_details values (%s,%s)", (int(row['University_ID']),row['University_Name']))
uni_recomm.commit()

cursor.execute("SELECT * from university_details")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[19]:


Student_df=pd.read_csv("Final_student.csv",encoding= 'unicode_escape')
Student_df


# In[22]:


for i,row in Student_df.iterrows():
    cursor.execute("INSERT INTO student values (%s,%s,%s,%s,%s,%s,%s)", (int(row['student_id']),row['University_ID'],row['Acceptance'],row['Program'],row['Target_Major'],row['Term'],row['Year_of_Joining']))
uni_recomm.commit()

cursor.execute("SELECT * from student")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[23]:


Recruiter_df=pd.read_csv("Final_recruiter.csv",encoding= 'unicode_escape')
Recruiter_df


# In[25]:


for i,row in Recruiter_df.iterrows():
    cursor.execute("INSERT INTO recruiter values (%s,%s,%s,%s,%s)", (int(row['Recruiter_ID']),row['Recruiter_Ranking'],row['Number_of_LCA'],row['Average_Salary'],row['University_ID']))
uni_recomm.commit()

cursor.execute("SELECT * from recruiter")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[26]:


RecruiterDetails_df=pd.read_csv("Final_RecruiterDetails.csv",encoding= 'unicode_escape')
RecruiterDetails_df


# In[27]:


for i,row in RecruiterDetails_df.iterrows():
    cursor.execute("INSERT INTO recruiter_details values (%s,%s)", (int(row['Recruiter_ID']),row['Recruiter_Name']))
uni_recomm.commit()

cursor.execute("SELECT * from recruiter_details")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[31]:


Scholarship_df=pd.read_csv("Final_scholarship.csv",encoding= 'unicode_escape')
Scholarship_df


# In[32]:


for i,row in Scholarship_df.iterrows():
    cursor.execute("INSERT INTO scholarship values (%s,%s,%s,%s)", (int(row['Scholarship_ID']),row['Scholarship_Name'],row['Scholarship_Url'],row['University_ID']))
uni_recomm.commit()

cursor.execute("SELECT * from scholarship")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[58]:


StudentScore_df=pd.read_csv("Student_Score.csv",encoding= 'unicode_escape')
StudentScore_df


# In[59]:


for i,row in StudentScore_df.iterrows():
    cursor.execute("INSERT INTO student_score values (%s,%s,%s,%s,%s,%s,%s)", (int(row['student_id']),row['GRE_Q'],row['GRE_V'],row['GRE_Total'],row['GRE_AWA'],row['TOEFL'],row['Work_Ex']))
uni_recomm.commit()

cursor.execute("SELECT * from student_score")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[35]:


Course_df=pd.read_csv("Final_course.csv")
Course_df


# In[36]:


for i,row in Course_df.iterrows():
    cursor.execute("INSERT INTO course values (%s,%s,%s)", (int(row['ID']),row['University_ID'],row['Course_Name']))
uni_recomm.commit()

cursor.execute("SELECT * from course")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[ ]:


#NORMALIZING THE TABLES TO REMOVE MULTI-VALUED ATTRIBUTES


# In[37]:


Professor_df=pd.read_csv("Professor_Final.csv")   #Multivalued rows
Professor_df


# In[38]:


lst_col = 'Professors'  


# In[41]:


x = Professor_df.assign(**{lst_col:Professor_df[lst_col].str.split(',')}) #Splitting the rows based on the delimtter ','
x


# In[42]:


import numpy as np


# In[43]:


pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]
#Inserting into new row the splitted values


# In[44]:


newProfessorsList_df = pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]

#Putting the new list into a dataframe


# In[45]:


newProfessorsList_df


# In[46]:


newProfessorsList_df['ID']= pd.Series(range(1,newProfessorsList_df.shape[0]+1)) #starts with 1
#Setting the student ID


# In[47]:


newProfessorsList_df


# In[48]:


for i,row in newProfessorsList_df.iterrows():
    cursor.execute("INSERT INTO professor values (%s,%s,%s)", (int(row['ID']),row['University_ID'],row['Professors']))
uni_recomm.commit()

cursor.execute("SELECT * from professor")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[49]:


NewResearch_df=pd.read_csv("Final_research.csv")
NewResearch_df


# In[50]:


lst_col = 'Research_Name'


# In[51]:


x = NewResearch_df.assign(**{lst_col:NewResearch_df[lst_col].str.split(',')})
x


# In[52]:


newResearchList_df = pd.DataFrame({
          col:np.repeat(x[col].values, x[lst_col].str.len())
          for col in x.columns.difference([lst_col])
      }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]
newResearchList_df


# In[53]:


newResearchList_df['Research_ID']= pd.Series(range(1,newResearchList_df.shape[0]+1)) #starts with 1


# In[54]:


newResearchList_df


# In[55]:


for i,row in newResearchList_df.iterrows():
    cursor.execute("INSERT INTO research values (%s,%s,%s)", (int(row['Research_ID']),row['Research_Name'],row['University_ID']))
uni_recomm.commit()

cursor.execute("SELECT * from research")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[56]:


UniversityAdmit_df=pd.read_csv("Final_universityAdmit.csv",encoding= 'unicode_escape')
UniversityAdmit_df


# In[57]:


for i,row in UniversityAdmit_df.iterrows():
    cursor.execute("INSERT INTO university_admit_requirement values (%s,%s,%s,%s)", (int(row['University_ID']),row['GRE'],row['toefl'],row['ielts']))
uni_recomm.commit()

cursor.execute("SELECT * from university_admit_requirement")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[60]:


StudentUndergrad_df=pd.read_csv("Student_Undergrad.csv",encoding= 'unicode_escape')
StudentUndergrad_df


# In[61]:


for i,row in StudentUndergrad_df.iterrows():
    cursor.execute("INSERT INTO student_undergrad values (%s,%s,%s,%s,%s)", (int(row['student_id']),row['Undergrad_University'],row['Undergrad_Major'],row['CGPA'],row['CGPA_Scale']))
uni_recomm.commit()

cursor.execute("SELECT * from student_undergrad")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[ ]:


#CLEANING THE DATASET


# In[64]:


#UPDATING THE VALUES IN THE STUDENT UNDERGRAD TABLE TO STANDARDIZE THE CGPA SCALE IN 10cgpa
cursor.execute("update student_undergrad set CGPA = CGPA/10 where CGPA_Scale = 100")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[65]:


cursor.execute("update student_undergrad set CGPA_Scale = 10 where CGPA_Scale = 100")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[ ]:


#DROPPING THE COLUMNS IN THE DATASET TO FIT THE NORMALIZED COLUMNS
cursor.execute("ALTER Table University drop column University_Name")
for x in cursor:
    print(x)


# In[ ]:


cursor.execute("ALTER Table recruiter drop column Recruiter_Name")
for x in cursor:
    print(x)


# In[ ]:


cursor.execute("ALTER Table Student drop column University_Name")
for x in cursor:
    print(x)


# In[ ]:


#DELETING THE MULTI-VALUED ATTRIBUTES FROM THE STUDENT TABLE.
#THIS HAS BEEN DONE BECAUSE THE STUDENT TABLE HAS MORE THAN 5000 VALUES, THEREFORE DELETING 470 VALUES WAS REMOVING
#MULTI-VALUED ATTRIBUTES AND REDUCING THE ACCURACY BY 4% ONLY.
cursor.execute("delete from student where Target_Major like '%/%'")

