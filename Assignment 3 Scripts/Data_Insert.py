#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


import pymysql
import mysql.connector
uni_recomm = pymysql.connect(host='localhost', user='root', passwd='RootPassword123', database='university_recommendation_assignment3')
cursor= uni_recomm.cursor()


# In[16]:


University_df=pd.read_csv("University_Data_Final.csv", encoding= 'unicode_escape')
University_df


# In[17]:


cursor.execute("Delete from university")


# In[18]:


cursor.execute("SELECT * from university")


# In[14]:


cursor.execute("describe university")
for x in cursor:
    print(x)


# In[19]:


for i,row in University_df.iterrows():
    cursor.execute("INSERT INTO university values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['University_ID']),row['University_Name'],row['avg_pay_scale'],row['Delivery_Mode'],row['Duration'],row['Term'],row['Year_of_Joining'],row['Major'],row['Location'],row['Cost_of_Living'],row['Avg_Fees'],row['GRE'],row['TOEFL'],row['IELTS']))
uni_recomm.commit()

cursor.execute("SELECT * from university")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[20]:


Scholarship_df=pd.read_csv("scholarship.csv")
Scholarship_df


# In[21]:


for i,row in Scholarship_df.iterrows():
    cursor.execute("INSERT INTO Scholarship values (%s,%s,%s)", (int(row['Scholarship_ID']),row['scholarship_name'],row['scholarship_url']))
uni_recomm.commit()

cursor.execute("SELECT * from Scholarship")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[90]:


Professor_df=pd.read_excel("university_professor.xlsx")
Professor_df


# In[89]:


for i,row in Professor_df.iterrows():
    cursor.execute("INSERT INTO Professor values (%s,%s,%s)", (int(row['ID']),row['University_ID'],row['Professors']))
uni_recomm.commit()

cursor.execute("SELECT * from Professor")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[85]:


cursor.execute("delete from professor")


# In[5]:


cursor.execute("describe university")
for x in cursor:
    print(x)


# In[25]:


Course_df=pd.read_excel("university_course.xlsx")
Course_df


# In[27]:


for i,row in Course_df.iterrows():
    cursor.execute("INSERT INTO course values (%s,%s,%s,%s)", (int(row['ID']),row['University_ID'],row['University_Name'],row['Course_Name']))
uni_recomm.commit()

cursor.execute("SELECT * from course")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[56]:


Recruiter_df=pd.read_excel("Recruiters_Data.xlsx")
Recruiter_df


# In[44]:


cursor.execute("describe recruiter")
for x in cursor:
    print(x)


# In[32]:


cursor.execute("Alter table recruiter modify column Average_Salary Varchar(255)")


# In[34]:


cursor.execute("drop table Recruiter")


# In[35]:


cursor.execute("CREATE TABLE Recruiter(Recruiter_ID Varchar(255),Recruiter_Name Varchar(255),Recruiter_Ranking INT,Number_of_LCA INT,Average_Salary VARCHAR(255),Max_Hiring_from_University Varchar(255),University_ID Varchar(255),PRIMARY KEY (Recruiter_ID),FOREIGN KEY (University_ID) REFERENCES university(University_ID))")


# In[57]:


cursor.execute("delete from Recruiter")


# In[46]:


cursor.execute("Describe Recruiter")
for x in cursor:
    print(x)


# In[60]:


cursor.execute("select * from recruiter")
for x in cursor:
    print(x)


# In[59]:


for i,row in Recruiter_df.iterrows():
    cursor.execute("INSERT INTO recruiter values (%s,%s,%s,%s,%s,%s,%s)", (int(row['Recruiter_ID']),row['Recruiter_Name'],row['Recruiter_Ranking'],row['Number_of_LCA'],row['Average_Salary'],row['Max_Hiring_from_University'],row['University_ID']))
uni_recomm.commit()

cursor.execute("SELECT * from recruiter")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[64]:


Student_df=pd.read_excel("student_data.xlsx")
Student_df


# In[67]:


cursor.execute("select * from student")


# In[66]:


for i,row in Student_df.iterrows():
    cursor.execute("INSERT INTO student values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (int(row['Student_ID']),row['University_Name'],row['University_ID'],row['Status'],row['Program'],row['Target_Major'],row['Term'],row['Year'],row['GRE_Q'],row['GRE_V'],row['GRE_Total'],row['GRE_AWA'],row['TOEFL'],row['Undergrad_University'],row['Undergrad_Major'],row['cgpa'],row['cgpaScale'],row['Work_Experience']))
uni_recomm.commit()

cursor.execute("SELECT * from student")
records=cursor.fetchall()

print(records)
uni_recomm.commit()


# In[21]:


cursor.execute("Alter table research modify column Research_Name longtext")


# In[71]:


cursor.execute("SELECT * from student")

for x in cursor:
    print(x)


# In[76]:


cursor.execute("Update student set CGPA = CGPA/10 WHERE CGPA_Scale = 100 AND UNIVERSITY_ID = 10")
for x in cursor:
    print(x)


# In[27]:


cursor.execute("SHOW PROCESSLIST")
for x in cursor:
    print(x)


# In[29]:


cursor.execute("Kill 19")

