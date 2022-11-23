from neo4j import GraphDatabase
import pandas as pd
#AURA_CONNECTION_URI = "neo4j+ssc://762d63d9.databases.neo4j.io"
AURA_CONNECTION_URI = "neo4j+ssc://3e5327eb.databases.neo4j.io"
AURA_USERNAME = "neo4j"
#AURA_PASSWORD = "3Bp6jyD1v_8WwXG3YwBADWruXCpVarrbdRyMi5ImOJU"
AURA_PASSWORD = "oN1jREJfpVn_5MaRT44BuewTH5DKhITftTUchiKJMQk"
# Driver instantiation
driver = GraphDatabase.driver(
    AURA_CONNECTION_URI,
    auth=(AURA_USERNAME, AURA_PASSWORD)
)

df = pd.read_excel("/Users/vamshireddy/Downloads/FALL2022/csc244/internjobs.xlsx")
jobname, city, company, country, state, degree, exp = "", "", "", "", "", "",-1
skills = []
description = ""
querys = []
c=0
for i in range(len(df)):
    description = df.loc[i]['Description']
    jobname = df.loc[i]['Job_Title']
    jobloc = df.loc[i]['Job_Location']
    if len(jobloc.split(",")) > 1:
        city = jobloc.split(",")[0].strip()
        state = jobloc.split(",")[1].strip()
    company = df.loc[i]['Company_Name']
    country = 'US'
    if ("jr" in jobname.lower()) or ("junior" in jobname.lower()) or ("undergraduate" in jobname.lower()) or ("bachelor" in jobname.lower()):
        degree,exp = "bachelors",0
    else:
        degree,exp = "masters",1
    skills = ["c","java","python"]
    if ("web" in jobname.lower()) or ("stack" in jobname.lower()):
        skills.extend(["html/css","javascript","react"])
    if ("data" in jobname.lower()) and ("analyst" in jobname.lower()):
        skills.extend(['tableau','r','sql','machine learning'])
    if ("data" in jobname.lower()) or ("science" in jobname.lower()):
        skills.extend(['deep learning','hadoop'])
    querys.append(f"merge (j: Job{{name:\"{jobname}\", company: \"{company}\", "
                  f"city: \"{city}\", state: \"{state}\", degree: \"{degree}\", experience: {exp}, skills: {skills}, "
                  f"description: \"{description}\"}})")
    # c+=1
    # for i in range(len(skills)):
    #     if c>50:
    #         break
    #     querys.append(f"MERGE (j:Job{{name:\"{jobname}\"}}) MERGE "
    #                   f"(s:Skill{{name:\"{skills[i]}\"}}) MERGE(j)-[:REQ_SKILL]->(s) return j,s")

#querydata = querys[:50]
with driver.session() as session:
    for q in querys:
        print(session.run(q).data())